// Thread Pool Visualizer - Interactive UI
// Professional WebSocket-based real-time visualization

class PoolVisualizer {
    constructor() {
        this.ws = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        this.reconnectDelay = 1000;
        this.charts = {};
        this.workerAnimations = new Map();
        this.valueAnimations = new WeakMap();
        this.poolConfig = {
            poolType: 'fixed',
            workerCount: 4,
            queueSize: 50,
            priority: 1,
            taskDuration: 100,
            batchSize: 1,
            schedulingMode: 'immediate',
            delayMs: 1000,
            cronExpression: '*/5 * * * * *',
            chainDependencies: false
        };
        this.appliedPoolConfig = { ...this.poolConfig };
        this.activePoolType = this.poolConfig.poolType;
        this.activeQueueCapacity = this.poolConfig.queueSize;
        this.apiKey = new URLSearchParams(window.location.search).get('api_key') || '';
        this.theme = localStorage.getItem('thread-pool-theme') || 'dark';
        this.prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
        this.hasCharts = typeof window.Chart === 'function';

        this.init();
    }

    init() {
        this.applyTheme(this.theme);
        this.setupEventListeners();
        this.initializeCharts();
        this.updateSchedulingControls();
        this.setConnectionState('connecting');
        this.connectWebSocket();
        this.startAnimationLoop();
    }

    // ==================== WebSocket Management ====================

    connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        let wsUrl = `${protocol}//${window.location.host}/api/v1/ws`;
        if (this.apiKey) {
            wsUrl += `?api_key=${encodeURIComponent(this.apiKey)}`;
        }

        try {
            this.ws = new WebSocket(wsUrl);

            this.ws.onopen = () => {
                console.log('✓ WebSocket connected');
                this.reconnectAttempts = 0;
                this.setConnectionState('connected');
                this.showNotification('Connected to server', 'success');
            };

            this.ws.onmessage = (event) => {
                try {
                    const message = JSON.parse(event.data);
                    if (message.type === 'state') {
                        this.updateUI(message.payload);
                    } else if (message.type === 'task_event') {
                        this.logTaskEvent(message.payload);
                    }
                } catch (err) {
                    console.error('Failed to parse message:', err);
                }
            };

            this.ws.onerror = (error) => {
                console.error('WebSocket error:', error);
            };

            this.ws.onclose = () => {
                console.log('WebSocket disconnected');
                this.setConnectionState('reconnecting');
                this.handleReconnect();
            };
        } catch (err) {
            console.error('Failed to create WebSocket:', err);
            this.handleReconnect();
        }
    }

    handleReconnect() {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            const delay = this.reconnectDelay * Math.pow(1.5, this.reconnectAttempts - 1);

            console.log(`Reconnecting in ${delay}ms... (attempt ${this.reconnectAttempts})`);
            this.showNotification(`Reconnecting... (${this.reconnectAttempts}/${this.maxReconnectAttempts})`, 'warning');

            setTimeout(() => this.connectWebSocket(), delay);
        } else {
            this.setConnectionState('offline');
            this.showNotification('Connection lost. Please refresh the page.', 'error');
        }
    }

    // ==================== UI Update & Rendering ====================

    updateUI(state) {
        if (!state) return;

        // Update metrics
        this.updateMetrics(state.metrics || {});

        // Update header stats
        this.updateHeaderStats(state.metrics || {});

        // Update worker grid
        this.updateWorkerGrid(state.workers || [], state.active_workers || 0);

        // Update queue visualization
        this.activePoolType = state.pool_type || this.activePoolType;
        this.activeQueueCapacity = state.queue_capacity || this.activeQueueCapacity;
        const activePoolDisplay = document.getElementById('active-pool-display');
        if (activePoolDisplay) {
            activePoolDisplay.textContent = `Current Pool: ${this.activePoolType}`;
        }
        this.updateSchedulingControls();
        this.updateQueue(state.queue_size || 0, this.activeQueueCapacity);
        this.setConnectionState('connected');

        // Update worker summary
        const activeCount = state.active_workers || 0;
        const totalCount = state.worker_count || 0;
        document.getElementById('active-count').textContent = activeCount;
        document.getElementById('idle-count').textContent = totalCount - activeCount;
        this.updateEmptyStates(state.workers || []);

        // Update charts
        this.updateCharts(state.metrics || {});
    }

    updateEmptyStates(workers) {
        const workersEmpty = document.getElementById('workers-empty-state');
        if (workersEmpty) {
            workersEmpty.classList.toggle('hidden', workers.length > 0);
        }
    }

    updateMetrics(metrics) {
        this.animateValue('tasks-submitted', metrics.tasks_submitted || 0);
        this.animateValue('tasks-completed', metrics.tasks_completed || 0);
        this.animateValue('throughput', metrics.throughput || 0);
        this.animateValue('avg-time', metrics.avg_exec_time_ms || 0);
    }

    updateHeaderStats(metrics) {
        this.animateValue('header-throughput', metrics.throughput || 0);
        const successRate = metrics.success_rate || 100;
        this.animateValue('header-success', successRate.toFixed(0));
    }

    updateWorkerGrid(workers, activeCount) {
        const grid = document.getElementById('workers-grid');

        // Sort workers by ID for consistent ordering
        workers.sort((a, b) => a.id - b.id);

        // Create or update worker cards
        workers.forEach((worker, index) => {
            let card = grid.children[index];

            if (!card) {
                card = this.createWorkerCard(worker);
                grid.appendChild(card);

                // Stagger animation for initial load
                setTimeout(() => {
                    card.style.opacity = '1';
                    card.style.transform = 'translateY(0) scale(1)';
                }, index * 50);
            } else {
                this.updateWorkerCard(card, worker);
            }
        });

        // Remove excess cards
        while (grid.children.length > workers.length) {
            const card = grid.lastChild;
            card.style.opacity = '0';
            card.style.transform = 'translateY(20px) scale(0.8)';
            setTimeout(() => grid.removeChild(card), 300);
        }
    }

    createWorkerCard(worker) {
        const card = document.createElement('div');
        card.className = 'worker-card glass-card';
        card.dataset.workerId = worker.id;
        card.style.opacity = '0';
        card.style.transform = 'translateY(20px) scale(0.8)';
        card.style.transition = 'all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)';

        const state = worker.state || 'idle';
        const stateClass = state === 'busy' ? 'busy' : 'idle';
        card.classList.toggle('busy', stateClass === 'busy');
        card.classList.toggle('idle', stateClass !== 'busy');

        card.innerHTML = `
            <div class="worker-header">
                <div class="worker-id">Worker ${worker.id}</div>
                <div class="worker-status ${stateClass}">
                    <span class="status-dot ${stateClass}"></span>
                    <span class="status-text">${state}</span>
                </div>
            </div>
            <div class="worker-stats">
                <div class="stat-row">
                    <span class="stat-label">Tasks</span>
                    <span class="stat-value">${worker.tasks_count || 0}</span>
                </div>
                ${worker.current_task ? `
                    <div class="stat-row current-task">
                        <span class="stat-label">Current</span>
                        <span class="stat-value">${worker.current_task}</span>
                    </div>
                ` : ''}
            </div>
        `;

        return card;
    }

    updateWorkerCard(card, worker) {
        const state = worker.state || 'idle';
        const stateClass = state === 'busy' ? 'busy' : 'idle';
        card.classList.toggle('busy', stateClass === 'busy');
        card.classList.toggle('idle', stateClass !== 'busy');

        const statusElement = card.querySelector('.worker-status');
        const statusDot = card.querySelector('.status-dot');
        const statusText = card.querySelector('.status-text');
        const tasksValue = card.querySelector('.worker-stats .stat-value');

        // Update state with animation
        if (statusElement.className !== `worker-status ${stateClass}`) {
            statusElement.className = `worker-status ${stateClass}`;
            statusDot.className = `status-dot ${stateClass}`;
            statusText.textContent = state;

            // Add pulse animation
            card.style.animation = 'none';
            setTimeout(() => {
                card.style.animation = state === 'busy' ? 'pulse 2s ease-in-out infinite' : '';
            }, 10);
        }

        // Update tasks count
        if (tasksValue) {
            this.animateValue(tasksValue, worker.tasks_count || 0);
        }

        // Update current task
        const currentTaskRow = card.querySelector('.current-task');
        if (worker.current_task && !currentTaskRow) {
            const statsDiv = card.querySelector('.worker-stats');
            const taskRow = document.createElement('div');
            taskRow.className = 'stat-row current-task';
            taskRow.innerHTML = `
                <span class="stat-label">Current</span>
                <span class="stat-value">${worker.current_task}</span>
            `;
            statsDiv.appendChild(taskRow);
        } else if (!worker.current_task && currentTaskRow) {
            currentTaskRow.remove();
        } else if (worker.current_task && currentTaskRow) {
            currentTaskRow.querySelector('.stat-value').textContent = worker.current_task;
        }
    }

    updateQueue(current, capacity) {
        const fill = document.getElementById('queue-fill');
        const currentEl = document.getElementById('queue-current');
        const capacityEl = document.getElementById('queue-capacity');
        const queueBar = document.getElementById('queue-bar');

        const percentage = capacity > 0 ? (current / capacity) * 100 : 0;
        fill.style.width = `${Math.min(percentage, 100)}%`;

        // Change color based on utilization
        if (percentage > 80) {
            fill.style.background = 'linear-gradient(90deg, #EC4899, #EF4444)';
        } else if (percentage > 50) {
            fill.style.background = 'linear-gradient(90deg, #F59E0B, #EC4899)';
        } else {
            fill.style.background = 'linear-gradient(90deg, #00D9FF, #8B5CF6)';
        }

        this.animateValue(currentEl, current);
        capacityEl.textContent = capacity;
        if (queueBar) {
            queueBar.setAttribute('aria-valuemax', String(capacity));
            queueBar.setAttribute('aria-valuenow', String(current));
        }

        const healthChip = document.getElementById('queue-health-chip');
        const queueSummary = document.getElementById('queue-summary');
        let health = 'Healthy';
        if (percentage > 80) {
            health = 'Near capacity';
        } else if (percentage > 50) {
            health = 'Elevated';
        } else if (percentage > 0) {
            health = 'Active';
        }
        if (healthChip) {
            healthChip.textContent = health;
            healthChip.dataset.state = health.toLowerCase().replace(/\s+/g, '-');
        }
        if (queueSummary) {
            queueSummary.textContent = capacity > 0
                ? `${Math.round(percentage)}% of queue capacity is currently in use.`
                : 'Queue is unbounded for the active pool.';
        }
    }

    animateValue(element, targetValue) {
        if (typeof element === 'string') {
            element = document.getElementById(element);
        }
        if (!element) return;

        if (this.prefersReducedMotion) {
            element.textContent = Math.round(targetValue);
            return;
        }

        const existingInterval = this.valueAnimations.get(element);
        if (existingInterval) {
            clearInterval(existingInterval);
        }

        const currentValue = parseFloat(element.textContent) || 0;
        const diff = targetValue - currentValue;
        const duration = 500;
        const steps = 30;
        const stepValue = diff / steps;
        const stepDuration = duration / steps;

        let currentStep = 0;

        const interval = setInterval(() => {
            currentStep++;
            const newValue = currentValue + (stepValue * currentStep);

            if (currentStep >= steps) {
                element.textContent = Math.round(targetValue);
                clearInterval(interval);
                this.valueAnimations.delete(element);
            } else {
                element.textContent = Math.round(newValue);
            }
        }, stepDuration);

        this.valueAnimations.set(element, interval);
    }

    // ==================== Charts ====================

    initializeCharts() {
        if (!this.hasCharts) {
            document.getElementById('throughput-summary').textContent = 'Throughput charts unavailable because Chart.js did not load.';
            document.getElementById('utilization-summary').textContent = 'Utilization charts unavailable because Chart.js did not load.';
            this.showNotification('Charts unavailable: continuing without Chart.js', 'warning');
            return;
        }

        const palette = this.getThemePalette();

        // Throughput chart
        const throughputCtx = document.getElementById('throughput-chart').getContext('2d');
        this.charts.throughput = new Chart(throughputCtx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Tasks/sec',
                    data: [],
                    borderColor: palette.cyan,
                    backgroundColor: `${palette.cyan}1a`,
                    borderWidth: 2,
                    tension: 0.4,
                    fill: true,
                    pointRadius: 0,
                    pointHitRadius: 10
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        backgroundColor: palette.tooltipBg,
                        titleColor: palette.cyan,
                        bodyColor: palette.textPrimary,
                        borderColor: palette.tooltipBorder,
                        borderWidth: 1,
                        padding: 12,
                        displayColors: false
                    }
                },
                scales: {
                    x: {
                        display: true,
                        ticks: {
                            color: palette.textSecondary,
                            font: { size: 10 },
                            maxRotation: 0,
                            minRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 6 // Show a reasonable number of time labels
                        },
                        grid: {
                            display: false
                        }
                    },
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: palette.grid,
                            drawBorder: false
                        },
                        ticks: {
                            color: palette.textSecondary,
                            font: { size: 11 }
                        }
                    }
                },
                interaction: {
                    mode: 'nearest',
                    axis: 'x',
                    intersect: false
                },
                animation: {
                    duration: this.prefersReducedMotion ? 0 : 300
                }
            }
        });

        // Utilization chart
        const utilizationCtx = document.getElementById('utilization-chart').getContext('2d');
        this.charts.utilization = new Chart(utilizationCtx, {
            type: 'doughnut',
            data: {
                labels: ['Active', 'Idle'],
                datasets: [{
                    data: [0, 100],
                    backgroundColor: [
                        `${palette.purple}cc`,
                        palette.chartIdle
                    ],
                    borderColor: [
                        palette.purple,
                        palette.grid
                    ],
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '70%',
                plugins: {
                    legend: {
                        display: true,
                        position: 'bottom',
                        labels: {
                            color: palette.textPrimary,
                            padding: 16,
                            font: { size: 12 }
                        }
                    },
                    tooltip: {
                        backgroundColor: palette.tooltipBg,
                        titleColor: palette.purple,
                        bodyColor: palette.textPrimary,
                        borderColor: palette.tooltipBorder,
                        borderWidth: 1,
                        padding: 12,
                        callbacks: {
                            label: (context) => {
                                return `${context.label}: ${context.parsed.toFixed(1)}%`;
                            }
                        }
                    }
                },
                animation: {
                    animateRotate: !this.prefersReducedMotion,
                    animateScale: !this.prefersReducedMotion
                }
            }
        });
    }

    updateCharts(metrics) {
        const throughputEmpty = document.getElementById('throughput-empty-state');
        const utilizationEmpty = document.getElementById('utilization-empty-state');
        if (!this.charts.throughput || !this.charts.utilization) {
            document.getElementById('throughput-summary').textContent = `Current throughput: ${metrics.throughput || 0} tasks per second.`;
            document.getElementById('utilization-summary').textContent = `Current worker utilization: ${(metrics.worker_utilization || 0).toFixed(1)} percent.`;
            const throughputChip = document.getElementById('throughput-chip');
            const utilizationChip = document.getElementById('utilization-chip');
            if (throughputChip) {
                throughputChip.textContent = metrics.throughput > 0 ? 'Live' : 'Waiting';
                throughputChip.dataset.state = metrics.throughput > 0 ? 'live' : 'waiting';
            }
            if (utilizationChip) {
                utilizationChip.textContent = (metrics.worker_utilization || 0) > 0 ? 'Active' : 'Idle';
                utilizationChip.dataset.state = (metrics.worker_utilization || 0) > 0 ? 'active' : 'idle';
            }
            if (throughputEmpty) throughputEmpty.classList.toggle('hidden', (metrics.throughput || 0) > 0);
            if (utilizationEmpty) utilizationEmpty.classList.toggle('hidden', (metrics.worker_utilization || 0) > 0);
            return;
        }

        const MAX_DATA_POINTS = 300; // 5 minutes of data at 1s intervals

        // Update throughput chart
        const now = new Date();
        const timeLabel = now.toLocaleTimeString('en-US', { hour12: false });

        const throughputData = this.charts.throughput.data;
        throughputData.labels.push(timeLabel);
        throughputData.datasets[0].data.push(metrics.throughput || 0);

        // Keep only last MAX_DATA_POINTS data points
        if (throughputData.labels.length > MAX_DATA_POINTS) {
            throughputData.labels.shift();
            throughputData.datasets[0].data.shift();
        }

        this.charts.throughput.update('none');
        document.getElementById('throughput-summary').textContent = `Current throughput: ${metrics.throughput || 0} tasks per second.`;
        const throughputChip = document.getElementById('throughput-chip');
        if (throughputChip) {
            throughputChip.textContent = metrics.throughput > 0 ? `${metrics.throughput} t/s` : 'Waiting';
            throughputChip.dataset.state = metrics.throughput > 0 ? 'live' : 'waiting';
        }
        if (throughputEmpty) throughputEmpty.classList.toggle('hidden', (metrics.throughput || 0) > 0);

        // Update utilization chart
        const workerUtil = metrics.worker_utilization || 0;
        this.charts.utilization.data.datasets[0].data = [
            workerUtil,
            100 - workerUtil
        ];
        this.charts.utilization.update('none');
        document.getElementById('utilization-summary').textContent = `Current worker utilization: ${workerUtil.toFixed(1)} percent.`;
        const utilizationChip = document.getElementById('utilization-chip');
        if (utilizationChip) {
            utilizationChip.textContent = workerUtil > 0 ? `${workerUtil.toFixed(0)}% busy` : 'Idle';
            utilizationChip.dataset.state = workerUtil > 0 ? 'active' : 'idle';
        }
        if (utilizationEmpty) utilizationEmpty.classList.toggle('hidden', workerUtil > 0);
    }

    // ==================== Event Handlers ====================

    getThemePalette() {
        const styles = getComputedStyle(document.body);
        return {
            textPrimary: styles.getPropertyValue('--text-primary').trim(),
            textSecondary: styles.getPropertyValue('--text-secondary').trim(),
            textDim: styles.getPropertyValue('--text-dim').trim(),
            grid: styles.getPropertyValue('--chart-grid').trim(),
            tooltipBg: styles.getPropertyValue('--tooltip-bg').trim(),
            tooltipBorder: styles.getPropertyValue('--tooltip-border').trim(),
            chartIdle: styles.getPropertyValue('--chart-idle').trim(),
            cyan: styles.getPropertyValue('--primary-cyan').trim(),
            purple: styles.getPropertyValue('--primary-purple').trim()
        };
    }

    applyTheme(theme) {
        this.theme = theme === 'light' ? 'light' : 'dark';
        document.body.classList.toggle('theme-light', this.theme === 'light');
        localStorage.setItem('thread-pool-theme', this.theme);

        const icon = document.getElementById('theme-toggle-icon');
        const label = document.getElementById('theme-toggle-label');
        const toggle = document.getElementById('theme-toggle');
        if (icon) icon.textContent = this.theme === 'light' ? '☀︎' : '☾';
        if (label) label.textContent = this.theme === 'light' ? 'Switch to Dark' : 'Switch to Light';
        if (toggle) toggle.setAttribute('aria-label', this.theme === 'light' ? 'Switch to dark theme' : 'Switch to light theme');

        this.refreshChartTheme();
    }

    refreshChartTheme() {
        const palette = this.getThemePalette();
        if (this.charts.throughput) {
            this.charts.throughput.data.datasets[0].borderColor = palette.cyan;
            this.charts.throughput.data.datasets[0].backgroundColor = `${palette.cyan}1a`;
            this.charts.throughput.options.plugins.tooltip.backgroundColor = palette.tooltipBg;
            this.charts.throughput.options.plugins.tooltip.titleColor = palette.cyan;
            this.charts.throughput.options.plugins.tooltip.bodyColor = palette.textPrimary;
            this.charts.throughput.options.plugins.tooltip.borderColor = palette.tooltipBorder;
            this.charts.throughput.options.scales.x.ticks.color = palette.textSecondary;
            this.charts.throughput.options.scales.y.ticks.color = palette.textSecondary;
            this.charts.throughput.options.scales.y.grid.color = palette.grid;
            this.charts.throughput.update('none');
        }

        if (this.charts.utilization) {
            this.charts.utilization.data.datasets[0].backgroundColor = [
                `${palette.purple}cc`,
                palette.chartIdle
            ];
            this.charts.utilization.data.datasets[0].borderColor = [
                palette.purple,
                palette.grid
            ];
            this.charts.utilization.options.plugins.legend.labels.color = palette.textPrimary;
            this.charts.utilization.options.plugins.tooltip.backgroundColor = palette.tooltipBg;
            this.charts.utilization.options.plugins.tooltip.titleColor = palette.purple;
            this.charts.utilization.options.plugins.tooltip.bodyColor = palette.textPrimary;
            this.charts.utilization.options.plugins.tooltip.borderColor = palette.tooltipBorder;
            this.charts.utilization.update('none');
        }
    }

    updateSchedulingControls() {
        const mode = this.poolConfig.schedulingMode;
        const delayGroup = document.getElementById('delay-group');
        const cronGroup = document.getElementById('cron-group');
        const chainGroup = document.getElementById('chain-group');
        const chainCheckbox = document.getElementById('chain-dependencies');
        const delayInput = document.getElementById('delay-ms');
        const cronInput = document.getElementById('cron-expression');
        const helper = document.getElementById('scheduling-helper');
        const submitButton = document.getElementById('submit-task');
        const submitContext = document.getElementById('submit-context');
        const scheduledReady = this.activePoolType === 'scheduled';
        const scheduledMode = mode !== 'immediate';

        delayGroup.classList.toggle('hidden', mode !== 'delayed');
        cronGroup.classList.toggle('hidden', mode !== 'recurring');
        chainGroup.classList.toggle('hidden', mode !== 'immediate');
        chainCheckbox.disabled = mode !== 'immediate';
        chainCheckbox.checked = this.poolConfig.chainDependencies;
        delayInput.disabled = mode !== 'delayed';
        cronInput.disabled = mode !== 'recurring';
        submitButton.disabled = scheduledMode && !scheduledReady;

        if (helper) {
            if (scheduledMode && !scheduledReady) {
                helper.textContent = 'Create a scheduled pool before submitting delayed or recurring work.';
            } else if (mode === 'delayed') {
                helper.textContent = 'Delayed runs wait for the configured delay before entering the scheduled pool.';
            } else if (mode === 'recurring') {
                helper.textContent = 'Recurring submissions use a 6-field cron expression and stay active until the pool is replaced.';
            } else {
                helper.textContent = 'Immediate batches can optionally chain task dependencies.';
            }
        }

        if (submitContext) {
            if (scheduledMode && !scheduledReady) {
                submitContext.textContent = 'Scheduling controls are locked until a scheduled pool is active.';
            } else if (mode === 'delayed') {
                submitContext.textContent = `Delayed submission will wait ${this.poolConfig.delayMs}ms before execution.`;
            } else if (mode === 'recurring') {
                submitContext.textContent = `Recurring submission uses cron: ${this.poolConfig.cronExpression}.`;
            } else if (this.poolConfig.chainDependencies) {
                submitContext.textContent = 'Each task in the batch will wait for the previous task to finish.';
            } else {
                submitContext.textContent = 'Immediate batches send directly to the active pool.';
            }
        }
    }

    setConnectionState(state) {
        const value = document.getElementById('connection-status-value');
        const pill = document.getElementById('connection-status');
        if (!value || !pill) return;

        const stateMap = {
            connected: 'Live',
            connecting: 'Connecting',
            reconnecting: 'Reconnecting',
            offline: 'Offline'
        };

        pill.dataset.state = state;
        value.textContent = stateMap[state] || 'Unknown';
        document.body.classList.toggle('is-stale', state !== 'connected');

        const banner = document.getElementById('status-banner');
        const bannerTitle = document.getElementById('status-banner-title');
        const bannerText = document.getElementById('status-banner-text');
        if (banner && bannerTitle && bannerText) {
            const showBanner = state !== 'connected';
            banner.classList.toggle('hidden', !showBanner);
            banner.dataset.state = state;
            if (state === 'offline') {
                bannerTitle.textContent = 'Offline';
                bannerText.textContent = 'Live data is unavailable. Controls may not reflect the actual server state until the connection returns.';
            } else if (state === 'reconnecting') {
                bannerTitle.textContent = 'Reconnecting';
                bannerText.textContent = 'Attempting to restore the live session. Displayed metrics may be stale until reconnection succeeds.';
            } else {
                bannerTitle.textContent = 'Connecting';
                bannerText.textContent = 'Establishing a live session with the thread pool service.';
            }
        }
    }

    setupSelectableGroup(selector, currentValue, onSelect) {
        const buttons = Array.from(document.querySelectorAll(selector));
        const syncState = (value) => {
            buttons.forEach((button) => {
                const active = String(button.dataset.pool ?? button.dataset.priority ?? button.dataset.mode) === String(value);
                button.classList.toggle('active', active);
                button.setAttribute('aria-checked', active ? 'true' : 'false');
                button.tabIndex = active ? 0 : -1;
            });
        };

        syncState(currentValue);
        buttons.forEach((button, index) => {
            button.addEventListener('click', () => {
                const value = button.dataset.pool ?? button.dataset.priority ?? button.dataset.mode;
                onSelect(value);
                syncState(value);
            });
            button.addEventListener('keydown', (event) => {
                if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(event.key)) return;
                event.preventDefault();
                const delta = (event.key === 'ArrowRight' || event.key === 'ArrowDown') ? 1 : -1;
                const nextIndex = (index + delta + buttons.length) % buttons.length;
                buttons[nextIndex].focus();
                buttons[nextIndex].click();
            });
        });
    }

    setupEventListeners() {
        const themeToggle = document.getElementById('theme-toggle');
        themeToggle.addEventListener('click', () => {
            this.applyTheme(this.theme === 'light' ? 'dark' : 'light');
        });

        this.setupSelectableGroup('.pool-btn', this.poolConfig.poolType, (value) => {
            this.poolConfig.poolType = value;
            this.updateSchedulingControls();
        });

        this.setupSelectableGroup('.mode-btn', this.poolConfig.schedulingMode, (value) => {
            this.poolConfig.schedulingMode = value;
            this.updateSchedulingControls();
        });

        this.setupSelectableGroup('.priority-btn', this.poolConfig.priority, (value) => {
            this.poolConfig.priority = parseInt(value, 10);
        });

        // Worker count slider
        const workerCountSlider = document.getElementById('worker-count');
        const workerCountDisplay = document.getElementById('worker-count-display');
        workerCountSlider.addEventListener('input', (e) => {
            this.poolConfig.workerCount = parseInt(e.target.value);
            workerCountDisplay.textContent = e.target.value;
        });

        // Queue size slider
        const queueSizeSlider = document.getElementById('queue-size');
        const queueSizeDisplay = document.getElementById('queue-size-display');
        queueSizeSlider.addEventListener('input', (e) => {
            this.poolConfig.queueSize = parseInt(e.target.value);
            queueSizeDisplay.textContent = e.target.value;
        });

        // Task duration slider
        const durationSlider = document.getElementById('task-duration');
        const durationDisplay = document.getElementById('duration-display');
        durationSlider.addEventListener('input', (e) => {
            this.poolConfig.taskDuration = parseInt(e.target.value);
            durationDisplay.textContent = e.target.value + 'ms';
        });

        // Batch size slider
        const batchSlider = document.getElementById('batch-size');
        const batchDisplay = document.getElementById('batch-size-display');
        batchSlider.addEventListener('input', (e) => {
            this.poolConfig.batchSize = parseInt(e.target.value);
            batchDisplay.textContent = e.target.value;
        });

        const delaySlider = document.getElementById('delay-ms');
        const delayDisplay = document.getElementById('delay-display');
        delaySlider.addEventListener('input', (e) => {
            this.poolConfig.delayMs = parseInt(e.target.value);
            delayDisplay.textContent = e.target.value + 'ms';
            this.updateSchedulingControls();
        });

        const cronInput = document.getElementById('cron-expression');
        cronInput.addEventListener('input', (e) => {
            this.poolConfig.cronExpression = e.target.value.trim();
            this.updateSchedulingControls();
        });

        const chainCheckbox = document.getElementById('chain-dependencies');
        chainCheckbox.addEventListener('change', (e) => {
            this.poolConfig.chainDependencies = e.target.checked;
            this.updateSchedulingControls();
        });

        // Create pool button
        document.getElementById('create-pool').addEventListener('click', () => {
            this.createPool();
        });

        // Submit task button
        document.getElementById('submit-task').addEventListener('click', () => {
            this.submitTask();
        });
    }

    // ==================== API Calls ====================

    apiURL(path) {
        if (!this.apiKey) {
            return path;
        }
        const separator = path.includes('?') ? '&' : '?';
        return `${path}${separator}api_key=${encodeURIComponent(this.apiKey)}`;
    }

    async createPool() {
        const btn = document.getElementById('create-pool');
        btn.disabled = true;
        btn.innerHTML = '<span>Creating...</span>';

        try {
            const response = await fetch(this.apiURL('/api/v1/pool/create'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    pool_type: this.poolConfig.poolType,
                    worker_count: this.poolConfig.workerCount,
                    queue_size: this.poolConfig.queueSize,
                    min_workers: Math.max(1, this.poolConfig.workerCount - 2),
                    max_workers: this.poolConfig.workerCount + 4
                })
            });

            if (!response.ok) throw new Error('Failed to create pool');

            this.appliedPoolConfig = { ...this.poolConfig };
            this.activePoolType = this.poolConfig.poolType;
            this.activeQueueCapacity = this.poolConfig.queueSize;
            this.updateSchedulingControls();
            this.showNotification(`${this.poolConfig.poolType} pool created with ${this.poolConfig.workerCount} workers`, 'success');
        } catch (err) {
            console.error('Create pool error:', err);
            this.showNotification('Failed to create pool', 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = `
                <span>Create Pool</span>
                <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M8 1v14M1 8h14" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                </svg>
            `;
        }
    }

    async submitTask() {
        const btn = document.getElementById('submit-task');
        btn.disabled = true;
        btn.innerHTML = '<span>Submitting...</span>';

        try {
            if (this.poolConfig.schedulingMode !== 'immediate' && this.activePoolType !== 'scheduled') {
                throw new Error('Delayed and recurring runs require a scheduled pool');
            }

            if (this.poolConfig.schedulingMode === 'recurring' && !this.poolConfig.cronExpression) {
                throw new Error('Cron expression is required for recurring runs');
            }

            const payload = {
                priority: this.poolConfig.priority,
                duration_ms: this.poolConfig.taskDuration,
                should_fail: false,
                task_count: this.poolConfig.batchSize
            };

            if (this.poolConfig.schedulingMode === 'delayed') {
                payload.delay_ms = this.poolConfig.delayMs;
            }
            if (this.poolConfig.schedulingMode === 'recurring') {
                payload.cron_expression = this.poolConfig.cronExpression;
            }
            if (this.poolConfig.schedulingMode === 'immediate' && this.poolConfig.chainDependencies) {
                payload.chain_dependencies = true;
            }

            const response = await fetch(this.apiURL('/api/v1/task/submit'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(errorText || 'Failed to submit task');
            }

            const result = await response.json();
            const modeLabel = this.poolConfig.schedulingMode === 'immediate'
                ? (this.poolConfig.chainDependencies ? 'chained batch' : 'immediate batch')
                : this.poolConfig.schedulingMode;
            this.showNotification(`Submitted ${result.count} task(s) as ${modeLabel}`, 'success');
        } catch (err) {
            console.error('Submit task error:', err);
            this.showNotification(err.message || 'Failed to submit task', 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = `
                <span>Submit Tasks</span>
                <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M1 8l14-7v14L1 8z" fill="currentColor"/>
                </svg>
            `;
            this.updateSchedulingControls();
        }
    }

    // ==================== Utilities ====================

    showNotification(message, type = 'info') {
        const liveRegion = document.getElementById('notification-live-region');
        if (liveRegion) {
            liveRegion.setAttribute('role', type === 'error' ? 'alert' : 'status');
            liveRegion.textContent = message;
        }

        const stack = document.getElementById('notification-stack');
        if (!stack) return;

        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        const styles = getComputedStyle(document.body);
        notification.style.cssText = `
            padding: 16px 24px;
            background: ${styles.getPropertyValue('--tooltip-bg').trim()};
            backdrop-filter: blur(20px);
            border: 1px solid ${type === 'success' ? '#10B981' : type === 'error' ? '#EF4444' : '#F59E0B'};
            border-radius: 12px;
            color: ${styles.getPropertyValue('--text-primary').trim()};
            font-size: 14px;
            animation: slideInRight 0.3s ease-out;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.3);
            pointer-events: auto;
        `;
        notification.textContent = message;

        stack.appendChild(notification);

        // Auto-remove after 3 seconds
        setTimeout(() => {
            notification.style.animation = 'slideOutRight 0.3s ease-out';
            setTimeout(() => notification.remove(), 300);
        }, 3000);
    }

    startAnimationLoop() {
        if (this.prefersReducedMotion) {
            return;
        }

        // Subtle background animation
        const orbs = document.querySelectorAll('.gradient-orb');
        let time = 0;

        const animate = () => {
            time += 0.01;
            orbs.forEach((orb, index) => {
                const offset = index * 2.1;
                const x = Math.sin(time + offset) * 30;
                const y = Math.cos(time + offset) * 30;
                orb.style.transform = `translate(${x}px, ${y}px)`;
            });

            requestAnimationFrame(animate);
        };

        animate();
    }

    logTaskEvent(event) {
        const logContainer = document.getElementById('task-log');
        if (!logContainer) return;
        const emptyState = document.getElementById('task-log-empty-state');
        if (emptyState) {
            emptyState.remove();
        }

        const { event_type, task_info } = event;

        const logEntry = document.createElement('div');
        logEntry.className = `log-entry log-${event_type}`;

        const timestamp = new Date().toLocaleTimeString('en-US', { hour12: false });
        
        logEntry.innerHTML = `
            <span class="log-timestamp">${timestamp}</span>
            <span class="log-task-id">${task_info.id}</span>
            <span class="log-event-type">${event_type}</span>
            <span class="log-duration">${task_info.duration_ms}ms</span>
        `;

        logContainer.prepend(logEntry);

        // Limit the number of log entries
        if (logContainer.children.length > 50) {
            logContainer.lastChild.remove();
        }
    }
}

// Initialize the visualizer when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        window.visualizer = new PoolVisualizer();
    });
} else {
    window.visualizer = new PoolVisualizer();
}

// Add notification animations
const style = document.createElement('style');
style.textContent = `
    @keyframes slideInRight {
        from {
            transform: translateX(400px);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }

    @keyframes slideOutRight {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(400px);
            opacity: 0;
        }
    }
    
    .task-log-container {
        display: flex;
        flex-direction: column;
    }
    .task-log {
        flex-grow: 1;
        overflow-y: auto;
        padding-right: 10px;
    }
    .log-entry {
        display: grid;
        grid-template-columns: minmax(72px, auto) minmax(0, 1fr) auto auto;
        gap: 8px;
        align-items: center;
        font-size: 12px;
        margin-bottom: 8px;
        padding: 4px 8px;
        border-radius: 4px;
        animation: fadeIn 0.3s ease;
    }
    .log-entry span {
        white-space: nowrap;
    }
    .log-timestamp { color: var(--text-dim); }
    .log-task-id { color: var(--text-secondary); min-width: 0; overflow: hidden; text-overflow: ellipsis; }
    .log-event-type { font-weight: 600; }
    .log-duration { color: var(--text-dim); justify-self: end; }

    .log-submitted { background-color: rgba(59, 130, 246, 0.1); }
    .log-submitted .log-event-type { color: #3b82f6; }
    .log-completed { background-color: rgba(16, 185, 129, 0.1); }
    .log-completed .log-event-type { color: #10b981; }
    .log-failed, .log-timeout { background-color: rgba(239, 68, 68, 0.1); }
    .log-failed .log-event-type, .log-timeout .log-event-type { color: #ef4444; }
    .log-cancelled { background-color: rgba(245, 158, 11, 0.1); }
    .log-cancelled .log-event-type { color: #f59e0b; }

    @media (max-width: 720px) {
        .log-entry {
            grid-template-columns: 1fr 1fr;
        }

        .log-task-id {
            grid-column: 1 / -1;
        }

        .log-duration {
            justify-self: start;
        }
    }
`;
document.head.appendChild(style);
