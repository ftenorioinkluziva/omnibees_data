let watchChart = null;
let debounceTimer = null;

async function api(path, opts = {}) {
    const res = await fetch(path, opts);
    return res.json();
}

function fmtPrice(n) {
    if (n == null) return '--';
    return 'R$ ' + n.toLocaleString('pt-BR', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}

function stars(n) {
    return n ? '\u2605'.repeat(n) : '';
}

Chart.defaults.color = '#71717a';
Chart.defaults.borderColor = '#232328';
Chart.defaults.font.family = "'IBM Plex Sans', sans-serif";
Chart.defaults.font.size = 11;

// ── Autocomplete ──

const searchInput = document.getElementById('hotel-search');
const resultsEl = document.getElementById('autocomplete-results');
const extIdInput = document.getElementById('hotel-ext-id');

searchInput.addEventListener('input', () => {
    clearTimeout(debounceTimer);
    const q = searchInput.value.trim();
    if (q.length < 2) {
        resultsEl.classList.remove('open');
        return;
    }
    debounceTimer = setTimeout(async () => {
        const results = await api(`/api/hotels-search?q=${encodeURIComponent(q)}`);
        if (results.length === 0) {
            resultsEl.classList.remove('open');
            return;
        }
        resultsEl.innerHTML = results.map(h => `
            <div class="ac-item" data-id="${h.external_id}" data-name="${h.name}">
                <div>
                    <div class="ac-item-name">${h.name}</div>
                    <div class="ac-item-meta">${h.city || ''} ${stars(h.stars)} ${h.chain ? '- ' + h.chain : ''}</div>
                </div>
            </div>
        `).join('');
        resultsEl.classList.add('open');

        resultsEl.querySelectorAll('.ac-item').forEach(item => {
            item.addEventListener('click', () => {
                searchInput.value = item.dataset.name;
                extIdInput.value = item.dataset.id;
                resultsEl.classList.remove('open');
            });
        });
    }, 250);
});

searchInput.addEventListener('blur', () => {
    setTimeout(() => resultsEl.classList.remove('open'), 200);
});

// ── Set default dates ──

const today = new Date();
const dateStartInput = document.getElementById('date-start');
const dateEndInput = document.getElementById('date-end');

dateStartInput.value = today.toISOString().split('T')[0];
const nextWeek = new Date(today);
nextWeek.setDate(nextWeek.getDate() + 7);
dateEndInput.value = nextWeek.toISOString().split('T')[0];

// ── Add watch ──

document.getElementById('btn-add').addEventListener('click', async () => {
    const extId = extIdInput.value;
    if (!extId) {
        searchInput.focus();
        searchInput.style.borderColor = '#ef4444';
        setTimeout(() => searchInput.style.borderColor = '', 1500);
        return;
    }

    const body = {
        hotel_external_id: extId,
        date_start: dateStartInput.value,
        date_end: dateEndInput.value,
        label: document.getElementById('label-input').value || null,
        target_price: parseFloat(document.getElementById('target-price').value) || null,
        notify: true,
    };

    const res = await api('/api/watchlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    });

    if (res.error) {
        alert(res.error);
        return;
    }

    searchInput.value = '';
    extIdInput.value = '';
    document.getElementById('label-input').value = '';
    document.getElementById('target-price').value = '';

    loadWatchlist();
});

// ── Load watchlist ──

async function loadWatchlist() {
    const data = await api('/api/watchlist');
    const grid = document.getElementById('watch-grid');

    if (data.length === 0) {
        grid.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">~</div>
                <div class="empty-text">Nenhum hotel monitorado</div>
                <div class="empty-sub">Busque um hotel acima para comecar</div>
            </div>
        `;
        return;
    }

    grid.innerHTML = data.map(w => {
        const hasTarget = w.target_price != null;
        const targetHit = hasTarget && w.min_price != null && w.min_price <= w.target_price;

        return `
        <div class="watch-card" data-watch-id="${w.id}">
            <div class="watch-card-actions">
                <button class="btn-icon btn-danger" data-delete="${w.id}" title="Remover">&times;</button>
            </div>
            <div class="watch-card-header">
                <div class="watch-card-name">${w.name}</div>
                ${w.label ? `<span class="watch-card-label">${w.label}</span>` : ''}
            </div>
            <div class="watch-card-meta">
                ${w.city || ''} ${w.stars ? stars(w.stars) : ''} ${w.chain ? '- ' + w.chain : ''}
                <br>${w.date_start} a ${w.date_end}
            </div>
            <div class="watch-card-prices">
                <div class="wc-stat">
                    <div class="wc-stat-value" style="color: var(--green)">${fmtPrice(w.min_price)}</div>
                    <div class="wc-stat-label">Menor</div>
                </div>
                <div class="wc-stat">
                    <div class="wc-stat-value">${fmtPrice(w.avg_price)}</div>
                    <div class="wc-stat-label">Media</div>
                </div>
                <div class="wc-stat">
                    <div class="wc-stat-value" style="color: var(--accent)">${fmtPrice(w.price_today)}</div>
                    <div class="wc-stat-label">Hoje</div>
                </div>
            </div>
            ${hasTarget ? `
            <div class="watch-card-target">
                <span class="target-info">Alvo: ${fmtPrice(w.target_price)}</span>
                <span class="target-status ${targetHit ? 'target-hit' : 'target-miss'}">
                    ${targetHit ? 'Atingido!' : 'Acima do alvo'}
                </span>
            </div>
            ` : ''}
        </div>
        `;
    }).join('');

    grid.querySelectorAll('.watch-card').forEach(card => {
        card.addEventListener('click', (e) => {
            if (e.target.closest('[data-delete]')) return;
            openWatchDetail(parseInt(card.dataset.watchId));
        });
    });

    grid.querySelectorAll('[data-delete]').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            e.stopPropagation();
            const id = btn.dataset.delete;
            await api(`/api/watchlist/${id}`, { method: 'DELETE' });
            loadWatchlist();
        });
    });
}

// ── Watch detail chart ──

async function openWatchDetail(watchId) {
    const data = await api(`/api/watchlist/${watchId}/prices`);
    if (data.error || !data.prices.length) return;

    const panel = document.getElementById('watch-detail');
    panel.style.display = 'block';
    panel.scrollIntoView({ behavior: 'smooth' });

    document.getElementById('wd-name').textContent = data.name;
    document.getElementById('wd-meta').textContent = `${data.date_start} a ${data.date_end}`;

    const ctx = document.getElementById('chart-watch').getContext('2d');
    if (watchChart) watchChart.destroy();

    const labels = data.prices.map(p => {
        const d = new Date(p.date + 'T00:00:00');
        return d.toLocaleDateString('pt-BR', { day: '2-digit', month: 'short' });
    });

    const datasets = [{
        label: 'Preco (R$)',
        data: data.prices.map(p => p.price),
        borderColor: '#f59e0b',
        backgroundColor: 'rgba(245, 158, 11, 0.08)',
        fill: true,
        tension: 0.3,
        pointRadius: 2,
        pointHoverRadius: 5,
        pointBackgroundColor: '#f59e0b',
        borderWidth: 2,
    }];

    if (data.target_price) {
        datasets.push({
            label: 'Alvo',
            data: data.prices.map(() => data.target_price),
            borderColor: '#22c55e',
            borderDash: [6, 4],
            borderWidth: 1.5,
            pointRadius: 0,
            fill: false,
        });
    }

    watchChart = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { intersect: false, mode: 'index' },
            plugins: {
                legend: {
                    display: !!data.target_price,
                    labels: { usePointStyle: true, pointStyleWidth: 8 }
                },
                tooltip: {
                    backgroundColor: '#18181b',
                    borderColor: '#232328',
                    borderWidth: 1,
                    bodyFont: { family: "'JetBrains Mono'", size: 12 },
                    callbacks: {
                        label: ctx => ctx.dataset.label + ': R$ ' + ctx.parsed.y.toLocaleString('pt-BR', { minimumFractionDigits: 2 })
                    }
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { maxRotation: 0, autoSkipPadding: 20, font: { size: 10 } }
                },
                y: {
                    grid: { color: '#1a1a1e' },
                    ticks: {
                        callback: v => 'R$ ' + v.toLocaleString('pt-BR'),
                        font: { size: 10 }
                    }
                }
            }
        }
    });
}

document.getElementById('btn-close-wd').addEventListener('click', () => {
    document.getElementById('watch-detail').style.display = 'none';
});

// ── Init ──
loadWatchlist();
