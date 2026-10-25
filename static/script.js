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
        liveValue: document.getElementById('live-value'),
        liveDate: document.getElementById('live-date'),
        statusDays: document.getElementById('status-days'),
        statusReadings: document.getElementById('status-readings'),
        statusReadingsWrap: document.getElementById('status-readings-wrap'),
        statusRange: document.getElementById('status-range'),
        statusLinznetz: document.getElementById('status-linznetz')
    };

    // Chart palette — read from CSS custom properties so the canvas follows the
    // Kennfarbe and the neutral ramp in both themes.
    function cssVar(name) {
        return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    }

    function withAlpha(color, alpha) {
        const match = /^#([0-9a-f]{6})$/i.exec(color);
        if (!match) return color;
        const value = parseInt(match[1], 16);
        return `rgba(${(value >> 16) & 255}, ${(value >> 8) & 255}, ${value & 255}, ${alpha})`;
    }

    function getChartColors() {
        return {
            accent: cssVar('--accent'),
            ink: cssVar('--ink'),
            inkMuted: cssVar('--ink-muted'),
            inkFaint: cssVar('--ink-faint'),
            line: cssVar('--line'),
            lineStrong: cssVar('--line-strong'),
            surface: cssVar('--surface')
        };
    }

    const chartFont = "'FW Grotesk', ui-sans-serif, system-ui, sans-serif";

    // API Helpers
    async function fetchData(url) {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
    }

    // Status bar: imported days, quarter-hour readings, and the date range.
    // Daily-only imports have no quarter-hour rows, so the reading count only
    // appears once there is something to count.
    function renderCounts(days, readings) {
        elements.statusDays.textContent = days.toLocaleString();
        elements.statusReadings.textContent = readings.toLocaleString();
        elements.statusReadingsWrap.hidden = readings === 0;
    }

    // After a reload the status bar would otherwise claim nothing was fetched.
    // Only the first load applies the stored result: later refreshes must not
    // overwrite what this session's own fetch just reported.
    function restoreFetchStatus(stats) {
        if (!stats.last_fetch_at) return;
        const when = new Date(stats.last_fetch_at).toLocaleString(undefined, {
            day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit'
        });
        const days = stats.last_fetch_days === 1 ? '1 day' : `${stats.last_fetch_days} days`;
        if (stats.last_fetch_outcome === 'failed') {
            setLinznetzStatus('fail', `LinzNetz fetch failed, ${when}`);
        } else if (stats.last_fetch_outcome === 'fetched') {
            setLinznetzStatus('ok', `LinzNetz fetched ${days}, ${when}`);
        } else {
            setLinznetzStatus('ok', `LinzNetz up to date, ${when}`);
        }
    }

    async function updateStats(restoreFetch = false) {
        try {
            const stats = await fetchData('/api/stats');
            if (restoreFetch) restoreFetchStatus(stats);
            renderCounts(
                typeof stats.total_days === 'number' ? stats.total_days : 0,
                typeof stats.total_readings === 'number' ? stats.total_readings : 0
            );
            if (stats.first_date && stats.last_date) {
                elements.statusRange.textContent = `${stats.first_date} – ${stats.last_date}`;
            } else {
                elements.statusRange.textContent = 'No data';
            }
        } catch (error) {
            console.error('Error fetching stats:', error);
            elements.statusDays.textContent = 'Unavailable';
            elements.statusReadingsWrap.hidden = true;
            elements.statusRange.textContent = 'Unavailable';
        }
    }

    function setLinznetzStatus(tone, message) {
        elements.statusLinznetz.className = `status status--${tone}`;
        elements.statusLinznetz.textContent = message;
    }

    // Status Messages
    function showStatus(message, type = 'info') {
        const tone = type === 'success' ? 'ok' : type === 'error' ? 'fail' : 'info';
        const line = document.createElement('p');
        line.className = `status status--${tone}`;
        line.textContent = message;

        elements.uploadStatus.appendChild(line);

        // Auto-remove after 5 seconds
        setTimeout(() => {
            line.style.opacity = '0';
            setTimeout(() => line.remove(), 300);
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

    const rangeButtons = () => [
        ['week', elements.rangeWeekButton],
        ['month', elements.rangeMonthButton],
        ['year', elements.rangeYearButton]
    ];

    function setPickedRange(range) {
        rangeButtons().forEach(([name, button]) => {
            if (button) button.setAttribute('aria-pressed', String(name === range));
        });
    }

    function updateRangeButtons(forceDisable = false) {
        const noData = forceDisable || !state.chart || state.chart.data.labels.length === 0;
        rangeButtons().forEach(([range, button]) => {
            if (button) button.disabled = noData || getLastNPoints(range) === null;
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
        setPickedRange(range);
    }

    function handleZoom(scale) {
        if (!state.chart) return;
        state.chart.zoom(scale);
        setPickedRange(null);
    }

    function panChart(delta) {
        if (!state.chart) return;
        state.chart.pan({x: delta}, undefined, 'none');
        setPickedRange(null);
    }

    function resetZoom() {
        if (!state.chart) return;
        state.chart.resetZoom();
        setPickedRange(null);
    }

    // Live strip: the last daily reading and its date.
    function updateLiveStrip(data) {
        if (!data.labels.length || !data.data.length) {
            elements.liveValue.textContent = 'No data';
            elements.liveDate.textContent = 'No date';
            return;
        }
        const last = data.labels.length - 1;
        elements.liveValue.textContent = `${Number(data.data[last]).toFixed(2)} kWh`;
        elements.liveDate.textContent = data.labels[last];
    }

    // Chart Rendering
    function renderChart(data) {
        if (state.chart) {
            state.chart.destroy();
            state.chart = null;
        }

        const hasData = data.labels.length > 0 && data.data.length > 0;
        elements.chartEmptyState.classList.toggle('hidden', hasData);

        if (state.currentAggregation === 'daily') {
            updateLiveStrip(hasData ? data : { labels: [], data: [] });
        }

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
            backgroundColor: isLineChart ? withAlpha(colors.accent, 0.15) : withAlpha(colors.accent, 0.75),
            borderColor: colors.accent,
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
                backgroundColor: withAlpha(colors.inkFaint, 0.2),
                borderColor: colors.inkFaint,
                borderWidth: 2,
                borderDash: [6, 4],
                pointRadius: 3,
                pointHoverRadius: 5,
                pointBackgroundColor: colors.inkFaint,
                pointBorderColor: colors.inkFaint,
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
                borderColor: colors.inkMuted,
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
                borderColor: colors.inkMuted,
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
                        display: false
                    },
                    tooltip: {
                        backgroundColor: colors.surface,
                        borderColor: colors.lineStrong,
                        borderWidth: 1,
                        titleColor: colors.ink,
                        titleFont: {
                            family: chartFont,
                            size: 13,
                            weight: 600
                        },
                        bodyColor: colors.inkMuted,
                        bodyFont: {
                            family: chartFont,
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
                                backgroundColor: withAlpha(colors.accent, 0.2),
                                borderColor: colors.accent,
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
                            color: colors.inkMuted,
                            font: {
                                family: chartFont,
                                size: 11
                            },
                            maxRotation: data.labels.length > 50 ? 45 : 0,
                            autoSkip: true,
                            autoSkipPadding: 10
                        },
                        grid: {
                            color: colors.line,
                            drawBorder: false
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: unit,
                            color: colors.inkMuted,
                            font: {
                                size: 12
                            }
                        },
                        ticks: {
                            color: colors.inkMuted,
                            font: {
                                family: chartFont,
                                size: 11
                            },
                            callback: function(value) {
                                return value.toFixed(1);
                            }
                        },
                        grid: {
                            color: colors.line,
                            drawBorder: false
                        }
                    }
                }
            }
        });
        updateZoomControls(false);
        setPickedRange(null);
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
            await updateStats();
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

    function summarizeFetch(results) {
        const imported = results.filter(result => result.status === 'success').length;
        const failed = results.some(result => result.status === 'error');
        if (failed) {
            setLinznetzStatus('fail', 'LinzNetz fetch failed');
        } else if (imported > 0) {
            const days = imported === 1 ? '1 day' : `${imported} days`;
            setLinznetzStatus('ok', `LinzNetz fetched ${days}`);
        } else {
            setLinznetzStatus('ok', 'LinzNetz up to date');
        }
    }

    async function handleFetchLatest() {
        clearStatus();
        elements.fetchButton.disabled = true;
        const originalLabel = elements.fetchButton.textContent;
        elements.fetchButton.textContent = 'Fetching…';
        setLinznetzStatus('info', 'Fetching from LinzNetz');

        try {
            const response = await fetch('/api/fetch', { method: 'POST' });
            if (response.status === 503) {
                showStatus('LinzNetz credentials not configured on the server.', 'error');
                setLinznetzStatus('warn', 'LinzNetz not configured');
                return;
            }
            if (response.status === 429) {
                showStatus('Too many requests — please try again later.', 'error');
                setLinznetzStatus('warn', 'LinzNetz rate limited');
                return;
            }
            if (!response.ok) {
                showStatus(`Fetch failed (HTTP ${response.status}).`, 'error');
                setLinznetzStatus('fail', `LinzNetz fetch failed (HTTP ${response.status})`);
                return;
            }
            const results = await response.json();
            results.forEach(renderImportResult);
            summarizeFetch(results);
            await updateChart();
            await updateStats();
        } catch (error) {
            showStatus('Fetch failed. Please try again.', 'error');
            setLinznetzStatus('fail', 'LinzNetz fetch failed');
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
            uploadTitle.textContent = files.length === 1 ? '1 file selected' : `${files.length} files selected`;
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

            state.flatpickrInstance = flatpickr(elements.datePickerInput, {
                dateFormat: "Y-m-d",
                defaultDate: defaultDate,
                maxDate: "today",
                onChange: () => {
                    if (state.currentAggregation === 'raw') updateChart();
                }
            });

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

            window.addEventListener('resize', () => {
                if (state.chart) state.chart.resize();
            });

            // Series colours come from the tokens, so a theme switch needs a redraw.
            window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
                updateChart();
            });

            setupDragAndDrop();

            await updateChart();
            await updateStats(true);
        } catch (error) {
            console.error('Initialization error:', error);
            showStatus('Initialization error. Please refresh the page.', 'error');
        }
    }

    initialize();
});
