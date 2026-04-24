document.addEventListener('DOMContentLoaded', () => {
    const state = {
        chart: null,
        currentAggregation: 'daily',
        flatpickrInstance: null
    };

    const elements = {
        chartContainer: document.getElementById('main-chart'),
        uploadForm: document.getElementById('upload-form'),
        uploadStatus: document.getElementById('upload-status'),
        uploadArea: document.getElementById('upload-area'),
        aggregationControls: document.querySelectorAll('input[name="aggregation"]'),
        datePickerContainer: document.getElementById('date-picker-container'),
        datePickerInput: document.getElementById('date-picker'),
        fileInput: document.getElementById('file-input'),
        statusBadge: document.querySelector('.status-badge .status-text')
    };

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
            if (stats && stats.total_readings) {
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

    // Chart Rendering
    function renderChart(data) {
        if (state.chart) {
            state.chart.destroy();
        }

        const ctx = elements.chartContainer.getContext('2d');
        const unit = state.currentAggregation === 'raw' ? 'W' : 'kWh';
        const isLineChart = state.currentAggregation === 'raw';

        const datasets = [];

        // Add actual data
        datasets.push({
            label: unit,
            type: isLineChart ? 'line' : 'bar',
            data: data.data,
            backgroundColor: 'rgba(0, 212, 170, 0.6)',
            borderColor: '#00d4aa',
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
                backgroundColor: 'rgba(255, 193, 7, 0.2)',
                borderColor: '#ffc107',
                borderWidth: 2,
                borderDash: [8, 4],
                pointRadius: 3,
                pointHoverRadius: 5,
                pointBackgroundColor: '#ffc107',
                pointBorderColor: '#ffc107',
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
                borderColor: '#ff6b6b',
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
                borderColor: '#ffa500',
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
                            color: '#94a3b8',
                            font: {
                                size: 12
                            },
                            padding: 16,
                            usePointStyle: true
                        }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0, 0, 0, 0.9)',
                        borderColor: '#00d4aa',
                        borderWidth: 1,
                        titleColor: '#fff',
                        titleFont: {
                            size: 13,
                            weight: 600
                        },
                        bodyColor: '#fff',
                        bodyFont: {
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
                                backgroundColor: 'rgba(0, 212, 170, 0.2)',
                                borderColor: '#00d4aa',
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
                            color: '#94a3b8',
                            font: {
                                size: 11
                            },
                            maxRotation: data.labels.length > 50 ? 45 : 0,
                            autoSkip: true,
                            autoSkipPadding: 10
                        },
                        grid: {
                            color: 'rgba(255, 255, 255, 0.05)',
                            drawBorder: false
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: unit,
                            color: '#94a3b8',
                            font: {
                                size: 12
                            }
                        },
                        ticks: {
                            color: '#94a3b8',
                            font: {
                                size: 11
                            },
                            callback: function(value) {
                                return value.toFixed(1);
                            }
                        },
                        grid: {
                            color: 'rgba(255, 255, 255, 0.05)',
                            drawBorder: false
                        }
                    }
                }
            }
        });
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

    // Aggregation Change
    function handleAggregationChange(event) {
        state.currentAggregation = event.target.value;

        if (state.currentAggregation === 'raw') {
            elements.datePickerContainer.classList.remove('hidden');
        } else {
            elements.datePickerContainer.classList.add('hidden');
        }

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
