document.addEventListener('DOMContentLoaded', () => {
    const state = {
        chart: null,
        currentAggregation: 'daily',
        flatpickrInstance: null
    };

    const elements = {
        chartContainer: document.getElementById('main-chart'),
        chartEmptyState: document.getElementById('chart-empty-state'),
        uploadForm: document.getElementById('upload-form'),
        uploadStatus: document.getElementById('upload-status'),
        uploadArea: document.getElementById('upload-area'),
        aggregationControls: document.querySelectorAll('input[name="aggregation"]'),
        datePickerContainer: document.getElementById('date-picker-container'),
        datePickerInput: document.getElementById('date-picker'),
        fileInput: document.getElementById('file-input'),
        fetchButton: document.getElementById('fetch-button'),
        rangeWeekButton: document.getElementById('range-week-button'),
        rangeMonthButton: document.getElementById('range-month-button'),
        rangeYearButton: document.getElementById('range-year-button'),
        panLeftButton: document.getElementById('pan-left-button'),
        panRightButton: document.getElementById('pan-right-button'),
        zoomInButton: document.getElementById('zoom-in-button'),
        zoomOutButton: document.getElementById('zoom-out-button'),
        zoomResetButton: document.getElementById('zoom-reset-button'),
        statusBadge: document.querySelector('.status-badge .status-text')
    };

    // Chart palette — read from CSS custom properties so the canvas never
    // drifts from the design tokens defined in styles.css.
    function cssVar(name) {
        return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    }

    function hexToRgba(hex, alpha) {
        const v = parseInt(hex.replace('#', ''), 16);
        return `rgba(${(v >> 16) & 255}, ${(v >> 8) & 255}, ${v & 255}, ${alpha})`;
    }

    function getChartColors() {
        return {
            copper: cssVar('--copper'),
            current: cssVar('--current'),
            gold: cssVar('--gold'),
            paperDim: cssVar('--paper-dim'),
            ink: cssVar('--ink'),
        };
    }

    // API Helpers
    async function fetchData(url) {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
    }

    // Update Entry Count Badge
    async function updateEntryCount() {
        try {
            const stats = await fetchData('/api/stats');
            if (stats && typeof stats.total_readings === 'number') {
                const count = stats.total_readings.toLocaleString();
                elements.statusBadge.textContent = `${count} entries`;
            }
        } catch (error) {
            console.error('Error fetching stats:', error);
        }
    }

    // Status Messages
    function showStatus(message, type = 'info') {
        const statusDiv = document.createElement('div');
        statusDiv.className = `status-message ${type}`;

        const icon = type === 'success' ? '✓' : type === 'error' ? '✗' : 'ⓘ';
        statusDiv.innerHTML = `
            <span class="status-icon">${icon}</span>
            <span>${message}</span>
        `;

        elements.uploadStatus.appendChild(statusDiv);

        // Auto-remove after 5 seconds
        setTimeout(() => {
            statusDiv.style.opacity = '0';
            setTimeout(() => statusDiv.remove(), 300);
        }, 5000);
    }

    function clearStatus() {
        elements.uploadStatus.innerHTML = '';
    }

    // Points to show per range × aggregation; null = disable the button
    const RANGE_POINTS = {
        week:  { daily: 7,   weekly: 2,  monthly: null, yearly: null },
        month: { daily: 31,  weekly: 5,  monthly: 1,    yearly: null },
        year:  { daily: 365, weekly: 52, monthly: 12,   yearly: 1    }
    };

    function getLastNPoints(range) {
        if (state.currentAggregation === 'raw') return null;
        return (RANGE_POINTS[range] || {})[state.currentAggregation] ?? null;
    }

    function updateRangeButtons(forceDisable = false) {
        const noData = forceDisable || !state.chart || state.chart.data.labels.length === 0;
        [['week', elements.rangeWeekButton], ['month', elements.rangeMonthButton], ['year', elements.rangeYearButton]].forEach(([range, btn]) => {
            if (btn) btn.disabled = noData || getLastNPoints(range) === null;
        });
    }

    function updateZoomControls(disabled) {
        [
            elements.panLeftButton,
            elements.panRightButton,
            elements.zoomInButton,
            elements.zoomOutButton,
            elements.zoomResetButton
        ].forEach(button => {
            if (button) button.disabled = disabled;
        });
        updateRangeButtons(disabled);
    }

    function zoomToRange(range) {
        if (!state.chart) return;
        const n = getLastNPoints(range);
        if (n === null) return;
        const count = state.chart.data.labels.length;
        if (count === 0) return;
        state.chart.zoomScale('x', { min: Math.max(0, count - n), max: count - 1 }, 'none');
    }

    function handleZoom(scale) {
        if (!state.chart) return;
        state.chart.zoom(scale);
    }

    function panChart(delta) {
        if (!state.chart) return;
        state.chart.pan({x: delta}, undefined, 'none');
    }

    function resetZoom() {
        if (!state.chart) return;
        state.chart.resetZoom();
    }

    // Chart Rendering
    function renderChart(data) {
        if (state.chart) {
            state.chart.destroy();
            state.chart = null;
        }

        const hasData = data.labels.length > 0 && data.data.length > 0;
        elements.chartEmptyState.classList.toggle('hidden', hasData);

        if (!hasData) {
            updateZoomControls(true);
            return;
        }

        const ctx = elements.chartContainer.getContext('2d');
        const unit = state.currentAggregation === 'raw' ? 'W' : 'kWh';
        const isLineChart = state.currentAggregation === 'raw';

        const colors = getChartColors();
        const datasets = [];

        // Add actual data
        datasets.push({
            label: unit,
            type: isLineChart ? 'line' : 'bar',
            data: data.data,
            backgroundColor: hexToRgba(colors.copper, 0.6),
            borderColor: colors.copper,
            borderWidth: 2,
            ...(isLineChart && {
                pointRadius: 0,
                pointHoverRadius: 4,
                tension: 0.4
            }),
            fill: false,
            order: 1
        });

        // Add forecast as line with area fill
        if (data.forecast && state.currentAggregation !== 'raw' && state.currentAggregation !== 'daily') {
            datasets.push({
                label: 'Forecast',
                type: 'line',
                data: data.forecast,
                backgroundColor: hexToRgba(colors.gold, 0.2),
                borderColor: colors.gold,
                borderWidth: 2,
                borderDash: [8, 4],
                pointRadius: 3,
                pointHoverRadius: 5,
                pointBackgroundColor: colors.gold,
                pointBorderColor: colors.gold,
                tension: 0.4,
                fill: true,
                order: 2
            });
        }

        // Add moving average
        const averageNames = {
            daily: '90-Day Average',
            weekly: '5-Week Average',
            monthly: '5-Month Average',
            yearly: '3-Year Average'
        };

        if (data.moving_average && averageNames[state.currentAggregation]) {
            datasets.push({
                label: averageNames[state.currentAggregation],
                type: 'line',
                data: data.moving_average,
                borderColor: colors.current,
                borderWidth: 2,
                pointRadius: 0,
                pointHoverRadius: 4,
                tension: 0.4,
                fill: false,
                order: 0
            });
        }

        // Add daily average pattern for raw view
        if (state.currentAggregation === 'raw' && data.daily_average_pattern) {
            datasets.push({
                label: 'Average Pattern',
                type: 'line',
                data: data.daily_average_pattern,
                borderColor: colors.current,
                borderWidth: 2,
                pointRadius: 0,
                pointHoverRadius: 4,
                tension: 0.4,
                fill: false
            });
        }

        state.chart = new Chart(ctx, {
            data: {
                labels: data.labels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            color: colors.paperDim,
                            font: {
                                family: "'IBM Plex Mono', ui-monospace, monospace",
                                size: 12
                            },
                            padding: 16,
                            usePointStyle: true
                        }
                    },
                    tooltip: {
                        backgroundColor: hexToRgba(colors.ink, 0.92),
                        borderColor: colors.copper,
                        borderWidth: 1,
                        titleColor: '#f4ecdf',
                        titleFont: {
                            family: "'IBM Plex Mono', ui-monospace, monospace",
                            size: 13,
                            weight: 600
                        },
                        bodyColor: '#f4ecdf',
                        bodyFont: {
                            family: "'IBM Plex Mono', ui-monospace, monospace",
                            size: 13
                        },
                        padding: 12,
                        displayColors: true,
                        callbacks: {
                            label: function(context) {
                                let label = context.dataset.label || '';
                                if (label) {
                                    label += ': ';
                                }
                                if (context.parsed.y !== null) {
                                    label += context.parsed.y.toFixed(2) + ' ' + unit;
                                }
                                return label;
                            }
                        }
                    },
                    zoom: {
                        zoom: {
                            wheel: {
                                enabled: true,
                                speed: 0.1
                            },
                            drag: {
                                enabled: true,
                                backgroundColor: hexToRgba(colors.copper, 0.2),
                                borderColor: colors.copper,
                                borderWidth: 1
                            },
                            mode: 'x'
                        },
                        pan: {
                            enabled: true,
                            mode: 'x'
                        },
                        limits: {
                            x: {min: 'original', max: 'original'}
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: {
                            color: colors.paperDim,
                            font: {
                                family: "'IBM Plex Mono', ui-monospace, monospace",
                                size: 11
                            },
                            maxRotation: data.labels.length > 50 ? 45 : 0,
                            autoSkip: true,
                            autoSkipPadding: 10
                        },
                        grid: {
                            color: cssVar('--line'),
                            drawBorder: false
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: unit,
                            color: colors.paperDim,
                            font: {
                                size: 12
                            }
                        },
                        ticks: {
                            color: colors.paperDim,
                            font: {
                                family: "'IBM Plex Mono', ui-monospace, monospace",
                                size: 11
                            },
                            callback: function(value) {
                                return value.toFixed(1);
                            }
                        },
                        grid: {
                            color: cssVar('--line'),
                            drawBorder: false
                        }
                    }
                }
            }
        });
        updateZoomControls(false);
    }

    // Update Chart Data
    async function updateChart() {
        try {
            let url = `/api/chart-data?aggregation=${state.currentAggregation}`;

            if (state.currentAggregation === 'raw') {
                const selectedDate = state.flatpickrInstance?.selectedDates[0];
                if (selectedDate) {
                    const year = selectedDate.getFullYear();
                    const month = String(selectedDate.getMonth() + 1).padStart(2, '0');
                    const day = String(selectedDate.getDate()).padStart(2, '0');
                    const dateStr = `${year}-${month}-${day}`;
                    url += `&day=${dateStr}`;
                } else {
                    renderChart({ labels: [], data: [] });
                    return;
                }
            }

            const chartData = await fetchData(url);
            renderChart(chartData);
        } catch (error) {
            console.error('Error loading chart:', error);
            showStatus('Error loading chart data', 'error');
        }
    }

    // File Upload
    async function handleUpload(event) {
        event.preventDefault();

        if (!elements.fileInput.files.length) {
            showStatus('Please select CSV files', 'error');
            return;
        }

        clearStatus();
        elements.uploadArea.classList.add('loading');

        const formData = new FormData();
        for (const file of elements.fileInput.files) {
            formData.append('files', file);
        }

        try {
            const response = await fetch('/api/import', {
                method: 'POST',
                body: formData
            });

            const results = await response.json();

            results.forEach(result => {
                if (result.status === 'error') {
                    showStatus(`${result.filename}: ${result.error}`, 'error');
                } else if (result.status === 'skipped') {
                    showStatus(`${result.filename}: Already imported`, 'info');
                } else {
                    showStatus(`${result.filename}: Imported successfully (${result.records_processed} records)`, 'success');
                }
            });

            await updateChart();
            await updateEntryCount();
        } catch (error) {
            showStatus('Upload failed. Please try again.', 'error');
        } finally {
            elements.uploadArea.classList.remove('loading');
            elements.uploadForm.reset();
            elements.uploadArea.querySelector('.upload-title').textContent = 'Drop CSV files here';
        }
    }

    function renderImportResult(result) {
        const label = result.filename || 'fetch';
        if (result.status === 'error') {
            showStatus(`${label}: ${result.error}`, 'error');
        } else if (result.status === 'skipped') {
            showStatus(`${label}: ${result.error || 'Already imported'}`, 'info');
        } else {
            const records = result.records_processed != null
                ? ` (${result.records_processed} records)`
                : '';
            showStatus(`${label}: Imported successfully${records}`, 'success');
        }
    }

    async function handleFetchLatest() {
        clearStatus();
        elements.fetchButton.disabled = true;
        const originalLabel = elements.fetchButton.textContent;
        elements.fetchButton.textContent = 'Fetching…';

        try {
            const response = await fetch('/api/fetch', { method: 'POST' });
            if (response.status === 503) {
                showStatus('LinzNetz credentials not configured on the server.', 'error');
                return;
            }
            if (response.status === 429) {
                showStatus('Too many requests — please try again later.', 'error');
                return;
            }
            if (!response.ok) {
                showStatus(`Fetch failed (HTTP ${response.status}).`, 'error');
                return;
            }
            const results = await response.json();
            results.forEach(renderImportResult);
            await updateChart();
            await updateEntryCount();
        } catch (error) {
            showStatus('Fetch failed. Please try again.', 'error');
        } finally {
            elements.fetchButton.disabled = false;
            elements.fetchButton.textContent = originalLabel;
        }
    }

    // Aggregation Change
    function handleAggregationChange(event) {
        state.currentAggregation = event.target.value;

        if (state.currentAggregation === 'raw') {
            elements.datePickerContainer.classList.remove('hidden');
        } else {
            elements.datePickerContainer.classList.add('hidden');
        }

        updateRangeButtons();
        updateChart();
    }

    // File Input Change (Auto-import)
    function handleFileInputChange() {
        const files = elements.fileInput.files;
        const uploadTitle = elements.uploadArea.querySelector('.upload-title');

        if (files.length > 0) {
            uploadTitle.textContent = `${files.length} file(s) selected`;
            handleUpload(new Event('submit'));
        } else {
            uploadTitle.textContent = 'Drop CSV files here';
        }
    }

    // Drag and Drop
    function setupDragAndDrop() {
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            elements.uploadArea.addEventListener(eventName, preventDefaults, false);
        });

        function preventDefaults(e) {
            e.preventDefault();
            e.stopPropagation();
        }

        ['dragenter', 'dragover'].forEach(eventName => {
            elements.uploadArea.addEventListener(eventName, () => {
                elements.uploadArea.classList.add('dragging');
            }, false);
        });

        ['dragleave', 'drop'].forEach(eventName => {
            elements.uploadArea.addEventListener(eventName, () => {
                elements.uploadArea.classList.remove('dragging');
            }, false);
        });

        elements.uploadArea.addEventListener('drop', (e) => {
            const dt = e.dataTransfer;
            const files = dt.files;
            elements.fileInput.files = files;
            handleFileInputChange();
        }, false);
    }

    // Initialize
    async function initialize() {
        try {
            // Get latest date for default
            let defaultDate = 'today';
            try {
                const data = await fetchData('/api/latest-date');
                if (data.latest_date) defaultDate = data.latest_date;
            } catch {
                // Use today as default date
            }

            // Initialize Flatpickr
            state.flatpickrInstance = flatpickr(elements.datePickerInput, {
                dateFormat: "Y-m-d",
                defaultDate: defaultDate,
                theme: "dark",
                maxDate: "today",
                onChange: () => {
                    if (state.currentAggregation === 'raw') updateChart();
                }
            });

            // Event listeners
            elements.uploadForm.addEventListener('submit', handleUpload);
            elements.aggregationControls.forEach(radio =>
                radio.addEventListener('change', handleAggregationChange)
            );
            elements.fileInput.addEventListener('change', handleFileInputChange);
            if (elements.fetchButton) {
                elements.fetchButton.addEventListener('click', handleFetchLatest);
            }
            if (elements.rangeWeekButton) {
                elements.rangeWeekButton.addEventListener('click', () => zoomToRange('week'));
            }
            if (elements.rangeMonthButton) {
                elements.rangeMonthButton.addEventListener('click', () => zoomToRange('month'));
            }
            if (elements.rangeYearButton) {
                elements.rangeYearButton.addEventListener('click', () => zoomToRange('year'));
            }
            if (elements.panLeftButton) {
                elements.panLeftButton.addEventListener('click', () => panChart(80));
            }
            if (elements.panRightButton) {
                elements.panRightButton.addEventListener('click', () => panChart(-80));
            }
            if (elements.zoomInButton) {
                elements.zoomInButton.addEventListener('click', () => handleZoom(1.25));
            }
            if (elements.zoomOutButton) {
                elements.zoomOutButton.addEventListener('click', () => handleZoom(0.8));
            }
            if (elements.zoomResetButton) {
                elements.zoomResetButton.addEventListener('click', resetZoom);
            }
            updateZoomControls(true);

            // Window resize
            window.addEventListener('resize', () => {
                if (state.chart) state.chart.resize();
            });

            // Drag and drop
            setupDragAndDrop();

            // Load initial chart and entry count
            await updateChart();
            await updateEntryCount();
        } catch (error) {
            console.error('Initialization error:', error);
            showStatus('Initialization error. Please refresh the page.', 'error');
        }
    }

    initialize();
});
