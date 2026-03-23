// Wikipedia Stream Dashboard - Frontend Logic

// Global State
let socket;
let topUsersChart;
let userGrowthChart;
let eventCount = 0;

// ============================================================================
// Initialization
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    connectWebSocket();
    setupEventListeners();
    
    // Initial data fetch
    updateSummary();
    updateTopStats();
    
    // Periodic updates for charts and summary
    setInterval(updateSummary, 5000);
    setInterval(updateTopStats, 10000);
});

function initCharts() {
    // Top Users Bar Chart
    const ctxTop = document.getElementById('topUsersChart').getContext('2d');
    topUsersChart = new Chart(ctxTop, {
        type: 'bar',
        data: {
            labels: [],
            datasets: [{
                label: 'Contributions',
                data: [],
                backgroundColor: 'rgba(52, 152, 219, 0.6)',
                borderColor: 'rgba(52, 152, 219, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { beginAtZero: true } }
        }
    });

    // User Growth Line Chart
    const ctxGrowth = document.getElementById('userGrowthChart').getContext('2d');
    userGrowthChart = new Chart(ctxGrowth, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Cumulative Contributions',
                data: [],
                borderColor: 'rgba(46, 204, 113, 1)',
                backgroundColor: 'rgba(46, 204, 113, 0.1)',
                fill: true,
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { 
                x: { title: { display: true, text: 'Time' } },
                y: { title: { display: true, text: 'Total' }, beginAtZero: false }
            }
        }
    });
}

function setupEventListeners() {
    document.getElementById('user-search-btn').addEventListener('click', () => {
        const username = document.getElementById('user-search-input').value.trim();
        if (username) analyzeUser(username);
    });

    document.getElementById('user-search-input').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            const username = document.getElementById('user-search-input').value.trim();
            if (username) analyzeUser(username);
        }
    });
}

// ============================================================================
// Real-Time Updates (WebSockets)
// ============================================================================

function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;
    
    socket = new WebSocket(wsUrl);
    
    socket.onopen = () => {
        console.log('WebSocket connected');
        updateConnectionStatus(true);
    };
    
    socket.onmessage = (event) => {
        const message = JSON.parse(event.data);
        if (message.type === 'live_event') {
            addEventToFeed(message.data);
        }
    };
    
    socket.onclose = () => {
        console.log('WebSocket disconnected');
        updateConnectionStatus(false);
        // Try to reconnect after 3 seconds
        setTimeout(connectWebSocket, 3000);
    };
}

function updateConnectionStatus(online) {
    const el = document.getElementById('connection-status');
    el.textContent = online ? 'Online' : 'Offline (Reconnecting...)';
    el.className = online ? 'status-online' : 'status-offline';
}

function addEventToFeed(data) {
    eventCount++;
    document.getElementById('feed-count').textContent = `${eventCount} events`;
    
    const feed = document.getElementById('live-feed');
    const item = document.createElement('div');
    item.className = 'event-item';
    
    const time = new Date(data.timestamp).toLocaleTimeString();
    const diffClass = data.diff >= 0 ? 'diff-plus' : 'diff-minus';
    const diffSign = data.diff >= 0 ? '+' : '';
    
    item.innerHTML = `
        <span class="meta">[${time}]</span> 
        <span class="user">${data.user}</span> 
        <span class="meta">edited</span> 
        <span class="topic">${data.title}</span>
        <span class="diff ${diffClass}">(${diffSign}${data.diff})</span>
        <div class="meta">${data.wiki} • ${data.edit_type.replace('_', ' ')}</div>
    `;
    
    // Remove placeholder if exists
    const placeholder = feed.querySelector('.placeholder');
    if (placeholder) placeholder.remove();
    
    feed.prepend(item);
    
    // Keep feed size manageable (last 50 items)
    if (feed.childNodes.length > 50) {
        feed.removeChild(feed.lastChild);
    }
}

// ============================================================================
// API Calls
// ============================================================================

async function updateSummary() {
    try {
        const response = await fetch('/api/summary');
        const data = await response.json();
        
        document.getElementById('stat-events').textContent = data.total_events.toLocaleString();
        document.getElementById('stat-users').textContent = data.total_users.toLocaleString();
        document.getElementById('stat-topics').textContent = data.total_topics.toLocaleString();
    } catch (err) {
        console.error('Error fetching summary:', err);
    }
}

async function updateTopStats() {
    try {
        // Fetch top active users
        const usersRes = await fetch('/api/active-users?period=hour');
        const usersData = await usersRes.json();
        
        // Update Chart
        topUsersChart.data.labels = usersData.map(u => u[0]);
        topUsersChart.data.datasets[0].data = usersData.map(u => u[1]);
        topUsersChart.update();
        
        // Fetch top typo topics
        const topicsRes = await fetch('/api/typo-topics');
        const topicsData = await topicsRes.json();
        
        const listEl = document.getElementById('typo-topics-list');
        listEl.innerHTML = topicsData.length > 0 ? '' : '<p class="placeholder">No data yet.</p>';
        
        topicsData.slice(0, 8).forEach(topic => {
            const p = document.createElement('p');
            p.innerHTML = `${topic[0]} <span class="count">${topic[1]}</span>`;
            listEl.appendChild(p);
        });
    } catch (err) {
        console.error('Error fetching top stats:', err);
    }
}

async function analyzeUser(username) {
    try {
        const response = await fetch(`/api/user/${encodeURIComponent(username)}?granularity=hour`);
        const data = await response.json();
        
        if (data.error) {
            document.getElementById('user-details').classList.add('hidden');
            document.getElementById('user-not-found').classList.remove('hidden');
            return;
        }
        
        // Display details
        document.getElementById('user-not-found').classList.add('hidden');
        document.getElementById('user-details').classList.remove('hidden');
        
        document.getElementById('detail-username').textContent = data.username;
        document.getElementById('detail-total').textContent = data.types.total;
        document.getElementById('detail-typos').textContent = data.types.typo_edits;
        document.getElementById('detail-content').textContent = data.types.content_additions;
        
        // Update topics tags
        const topicsEl = document.getElementById('detail-topics');
        topicsEl.innerHTML = '';
        data.top_topics.forEach(topic => {
            const li = document.createElement('li');
            li.textContent = `${topic[0]} (${topic[1]})`;
            topicsEl.appendChild(li);
        });
        
        // Update Growth Chart
        userGrowthChart.data.labels = data.series.map(p => new Date(p[0]).toLocaleTimeString());
        userGrowthChart.data.datasets[0].data = data.series.map(p => p[1]);
        userGrowthChart.update();
        
    } catch (err) {
        console.error('Error analyzing user:', err);
        document.getElementById('user-details').classList.add('hidden');
        document.getElementById('user-not-found').classList.remove('hidden');
    }
}
