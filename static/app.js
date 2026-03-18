const API = '';
let currentPage = 0;
const PAGE_SIZE = 30;
let currentFilters = {};
let priceChart = null;
let compareChart = null;
let distChart = null;
let selectedHotel = null;

// ── API calls ──

async function api(path) {
    const res = await fetch(`${API}${path}`);
    return res.json();
}

// ── Format helpers ──

function fmt(n) {
    if (n == null) return '--';
    return n.toLocaleString('pt-BR');
}

function fmtPrice(n) {
    if (n == null) return '--';
    return 'R$ ' + n.toLocaleString('pt-BR', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}

function fmtCompact(n) {
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
    if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
    return n.toString();
}

function stars(n) {
    return n ? '\u2605'.repeat(n) : '-';
}

// ── Chart defaults ──

Chart.defaults.color = '#71717a';
Chart.defaults.borderColor = '#232328';
Chart.defaults.font.family = "'IBM Plex Sans', sans-serif";
Chart.defaults.font.size = 11;

const CHART_COLORS = ['#f59e0b', '#06b6d4', '#a78bfa', '#22c55e', '#ef4444', '#ec4899', '#f97316', '#14b8a6', '#8b5cf6', '#64748b'];

// ── Load Stats ──

async function loadStats() {
    const s = await api('/api/stats');
    document.getElementById('kpi-hotels').textContent = fmt(s.hotels);
    document.getElementById('kpi-chains').textContent = fmt(s.chains);
    document.getElementById('kpi-diarias').textContent = fmtCompact(s.diarias);
    document.getElementById('kpi-avg').textContent = s.avg_price_today ? fmtPrice(s.avg_price_today) : '--';
    document.getElementById('kpi-changes').textContent = fmtCompact(s.historico);
    document.getElementById('last-update').textContent = `${s.date_min || '?'} a ${s.date_max || '?'}`;
}

// ── Load Cities ──

async function loadCities() {
    const cities = await api('/api/top-cities?limit=15');
    const el = document.getElementById('cities-list');
    const maxCount = Math.max(...cities.map(c => c.hotels));

    el.innerHTML = cities.map(c => `
        <div class="city-row" data-city="${c.city}">
            <div>
                <div class="city-name">${c.city} <span class="city-state">${c.state || ''}</span></div>
                <div class="city-bar"><div class="city-bar-fill" style="width: ${(c.hotels / maxCount * 100)}%"></div></div>
            </div>
            <span class="city-count">${c.hotels}</span>
            <span class="city-price">${c.avg_price ? fmtPrice(c.avg_price) : '--'}</span>
        </div>
    `).join('');

    el.querySelectorAll('.city-row').forEach(row => {
        row.addEventListener('click', () => {
            document.getElementById('search-input').value = '';
            document.getElementById('filter-state').value = '';
            document.getElementById('filter-stars').value = '';
            document.getElementById('filter-chain').value = '';
            currentFilters = { city: row.dataset.city };
            currentPage = 0;
            loadHotels();
        });
    });
}

// ── Load Distribution ──

async function loadDistribution() {
    const data = await api('/api/price-distribution');
    const ctx = document.getElementById('chart-distribution').getContext('2d');

    if (distChart) distChart.destroy();

    distChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: data.map(d => d.range),
            datasets: [{
                data: data.map(d => d.count),
                backgroundColor: ['#22c55e', '#06b6d4', '#f59e0b', '#a78bfa', '#ef4444'],
                borderColor: '#0a0a0b',
                borderWidth: 2,
                hoverOffset: 6,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '62%',
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        padding: 14,
                        usePointStyle: true,
                        pointStyleWidth: 8,
                        font: { size: 11, family: "'IBM Plex Sans'" }
                    }
                }
            }
        }
    });
}

// ── Load Filters ──

async function loadFilters() {
    const f = await api('/api/filters');

    const stateEl = document.getElementById('filter-state');
    f.states.forEach(s => {
        const o = document.createElement('option');
        o.value = s; o.textContent = s;
        stateEl.appendChild(o);
    });

    const starsEl = document.getElementById('filter-stars');
    f.stars.forEach(s => {
        const o = document.createElement('option');
        o.value = s; o.textContent = s + ' estrelas';
        starsEl.appendChild(o);
    });

    const chainEl = document.getElementById('filter-chain');
    f.chains.forEach(c => {
        const o = document.createElement('option');
        o.value = c; o.textContent = c.length > 30 ? c.slice(0, 30) + '...' : c;
        chainEl.appendChild(o);
    });
}

// ── Load Hotels ──

async function loadHotels() {
    const params = new URLSearchParams();
    params.set('limit', PAGE_SIZE);
    params.set('offset', currentPage * PAGE_SIZE);

    if (currentFilters.city) params.set('city', currentFilters.city);
    if (currentFilters.state) params.set('state', currentFilters.state);
    if (currentFilters.stars) params.set('stars', currentFilters.stars);
    if (currentFilters.chain) params.set('chain', currentFilters.chain);
    if (currentFilters.search) params.set('search', currentFilters.search);

    const data = await api(`/api/hotels?${params}`);
    const tbody = document.getElementById('hotels-tbody');

    tbody.innerHTML = data.hotels.map(h => `
        <tr data-id="${h.external_id}">
            <td title="${h.name}">${h.name}</td>
            <td>${h.city || '--'}</td>
            <td>${h.state || '--'}</td>
            <td class="col-stars">${stars(h.stars)}</td>
            <td>${h.chain ? (h.chain.length > 22 ? h.chain.slice(0, 22) + '...' : h.chain) : '--'}</td>
            <td class="col-price">${h.price_today ? fmtPrice(h.price_today) : '--'}</td>
        </tr>
    `).join('');

    document.getElementById('table-count').textContent = `${fmt(data.total)} hoteis`;
    const totalPages = Math.ceil(data.total / PAGE_SIZE);
    document.getElementById('page-info').textContent = `${currentPage + 1} / ${totalPages || 1}`;
    document.getElementById('btn-prev').disabled = currentPage === 0;
    document.getElementById('btn-next').disabled = currentPage >= totalPages - 1;

    tbody.querySelectorAll('tr').forEach(row => {
        row.addEventListener('click', () => openDetail(row.dataset.id));
    });
}

// ── Hotel Detail ──

const AMENITY_ICONS = {
    general: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/></svg>`,
    food: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 8h1a4 4 0 010 8h-1M2 8h16v9a4 4 0 01-4 4H6a4 4 0 01-4-4V8z"/></svg>`,
    wellness: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20.84 4.61a5.5 5.5 0 00-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 00-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 000-7.78z"/></svg>`,
    events: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>`,
};

const AMENITY_LABELS = {
    general: 'Geral',
    amenities_general: 'Geral',
    food: 'Gastronomia',
    amenities_food: 'Gastronomia',
    wellness: 'Bem-estar',
    amenities_wellness: 'Bem-estar',
    events: 'Eventos',
    amenities_events: 'Eventos',
};

function normalizeCategory(key) {
    return key.replace('amenities_', '');
}

function openLightbox(src) {
    const overlay = document.createElement('div');
    overlay.className = 'lightbox-overlay';
    overlay.innerHTML = `<img src="${src}" />`;
    overlay.addEventListener('click', () => overlay.remove());
    document.body.appendChild(overlay);
}

function renderPriceStats(prices) {
    if (prices.length > 0) {
        const values = prices.map(p => p.price);
        const min = Math.min(...values);
        const max = Math.max(...values);
        const avg = values.reduce((a, b) => a + b, 0) / values.length;
        const minDate = prices[values.indexOf(min)].date;

        document.getElementById('detail-stats').innerHTML = `
            <div class="detail-stat">
                <div class="detail-stat-value" style="color: var(--green)">${fmtPrice(min)}</div>
                <div class="detail-stat-label">Menor preco</div>
            </div>
            <div class="detail-stat">
                <div class="detail-stat-value" style="color: var(--red)">${fmtPrice(max)}</div>
                <div class="detail-stat-label">Maior preco</div>
            </div>
            <div class="detail-stat">
                <div class="detail-stat-value">${fmtPrice(avg)}</div>
                <div class="detail-stat-label">Media</div>
            </div>
            <div class="detail-stat">
                <div class="detail-stat-value">${minDate}</div>
                <div class="detail-stat-label">Data mais barata</div>
            </div>
        `;
        renderPriceChart(prices);
    } else {
        document.getElementById('detail-stats').innerHTML = '<p style="color:var(--text-dim)">Sem dados de preco disponíveis</p>';
        if (priceChart) { priceChart.destroy(); priceChart = null; }
    }
}

async function loadPrices(externalId, dateFrom, dateTo) {
    let url = `/api/hotels/${externalId}/prices`;
    if (dateFrom && dateTo) {
        url += `?date_from=${dateFrom}&date_to=${dateTo}`;
    } else {
        url += '?days=90';
    }
    const data = await api(url);
    renderPriceStats(data.prices);
}

async function openDetail(externalId) {
    selectedHotel = externalId;
    const [hotel, pricesData] = await Promise.all([
        api(`/api/hotels/${externalId}`),
        api(`/api/hotels/${externalId}/prices?days=90`)
    ]);

    if (hotel.error) return;

    const panel = document.getElementById('detail-panel');
    panel.style.display = 'block';
    panel.scrollIntoView({ behavior: 'smooth', block: 'start' });

    document.getElementById('detail-name').textContent = hotel.name;

    const metaParts = [
        hotel.city, hotel.state, hotel.chain,
        hotel.stars ? stars(hotel.stars) : null
    ].filter(Boolean);
    document.getElementById('detail-meta').textContent = metaParts.join(' \u00b7 ');

    document.getElementById('price-date-from').value = '';
    document.getElementById('price-date-to').value = '';

    renderInfoStrip(hotel);
    renderDescription(hotel);
    renderGallery(hotel);
    renderRoomTypes(hotel);
    renderAmenities(hotel);
    renderPriceStats(pricesData.prices);

    document.getElementById('compare-section').style.display = 'none';
}

function renderInfoStrip(hotel) {
    const el = document.getElementById('detail-info-strip');
    const tags = [];

    if (hotel.address) {
        tags.push(`<span class="detail-info-tag">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z"/><circle cx="12" cy="10" r="3"/></svg>
            ${hotel.address}${hotel.zip_code ? ', ' + hotel.zip_code : ''}
        </span>`);
    }

    if (hotel.check_in) {
        tags.push(`<span class="detail-info-tag">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
            Check-in ${hotel.check_in}
        </span>`);
    }

    if (hotel.check_out) {
        tags.push(`<span class="detail-info-tag">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 8 14"/></svg>
            Check-out ${hotel.check_out}
        </span>`);
    }

    if (hotel.phone) {
        tags.push(`<span class="detail-info-tag">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 16.92v3a2 2 0 01-2.18 2 19.79 19.79 0 01-8.63-3.07 19.5 19.5 0 01-6-6 19.79 19.79 0 01-3.07-8.67A2 2 0 014.11 2h3a2 2 0 012 1.72c.127.96.361 1.903.7 2.81a2 2 0 01-.45 2.11L8.09 9.91a16 16 0 006 6l1.27-1.27a2 2 0 012.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0122 16.92z"/></svg>
            ${hotel.phone}
        </span>`);
    }

    if (hotel.email) {
        tags.push(`<span class="detail-info-tag">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>
            <a href="mailto:${hotel.email}">${hotel.email}</a>
        </span>`);
    }

    if (hotel.latitude && hotel.longitude) {
        const mapsUrl = `https://www.google.com/maps?q=${hotel.latitude},${hotel.longitude}`;
        tags.push(`<span class="detail-info-tag">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="3 11 22 2 13 21 11 13 3 11"/></svg>
            <a href="${mapsUrl}" target="_blank" rel="noopener">Ver no mapa</a>
        </span>`);
    }

    if (hotel.external_id) {
        const omnibUrl = `https://book.omnibees.com/hotel/${hotel.external_id}`;
        tags.push(`<span class="detail-info-tag">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
            <a href="${omnibUrl}" target="_blank" rel="noopener">Omnibees</a>
        </span>`);
    }

    el.innerHTML = tags.join('');
    el.style.display = tags.length ? 'flex' : 'none';
}

function renderDescription(hotel) {
    const el = document.getElementById('detail-description');
    const desc = (hotel.description || '').replace(/LER MAIS$/i, '').trim();
    if (desc) {
        el.textContent = desc;
        el.style.display = 'block';
    } else {
        el.style.display = 'none';
    }
}

function renderGallery(hotel) {
    const el = document.getElementById('detail-gallery');
    const allImages = [];

    if (hotel.images && hotel.images.length) {
        hotel.images.forEach(url => allImages.push(url));
    }

    if (hotel.room_types && hotel.room_types.length) {
        hotel.room_types.forEach(rt => {
            if (rt.image_url && !allImages.includes(rt.image_url)) {
                allImages.push(rt.image_url);
            }
        });
    }

    if (allImages.length) {
        el.innerHTML = allImages.map(url =>
            `<img src="${url}" alt="" loading="lazy" onclick="openLightbox('${url}')" />`
        ).join('');
        el.style.display = 'flex';
    } else {
        el.style.display = 'none';
    }
}

function renderRoomTypes(hotel) {
    const container = document.getElementById('detail-rooms');
    const grid = document.getElementById('detail-rooms-grid');

    if (!hotel.room_types || !hotel.room_types.length) {
        container.style.display = 'none';
        return;
    }

    grid.innerHTML = hotel.room_types.map(rt => `
        <div class="detail-room-card">
            ${rt.image_url ? `<img src="${rt.image_url}" alt="${rt.name}" loading="lazy" />` : ''}
            <div class="room-name" title="${rt.name}">${rt.name}</div>
        </div>
    `).join('');
    container.style.display = 'block';
}

function renderAmenities(hotel) {
    const container = document.getElementById('detail-amenities');
    const grid = document.getElementById('detail-amenities-grid');

    if (!hotel.amenities || typeof hotel.amenities !== 'object') {
        container.style.display = 'none';
        return;
    }

    const groups = Object.entries(hotel.amenities)
        .filter(([, items]) => Array.isArray(items) && items.length > 0);

    if (!groups.length) {
        container.style.display = 'none';
        return;
    }

    grid.innerHTML = groups.map(([key, items]) => {
        const cat = normalizeCategory(key);
        const label = AMENITY_LABELS[key] || cat;
        return `
            <div class="amenity-group" data-category="${cat}">
                <div class="amenity-group-label">${label}</div>
                <div class="amenity-group-items">
                    ${items.map(item => `<span class="amenity-pill">${item}</span>`).join('')}
                </div>
            </div>
        `;
    }).join('');
    container.style.display = 'block';
}

function renderPriceChart(prices) {
    const ctx = document.getElementById('chart-prices').getContext('2d');
    if (priceChart) priceChart.destroy();

    const labels = prices.map(p => {
        const d = new Date(p.date + 'T00:00:00');
        return d.toLocaleDateString('pt-BR', { day: '2-digit', month: 'short' });
    });

    priceChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: 'Preco (R$)',
                data: prices.map(p => p.price),
                borderColor: '#f59e0b',
                backgroundColor: 'rgba(245, 158, 11, 0.08)',
                fill: true,
                tension: 0.3,
                pointRadius: 0,
                pointHoverRadius: 5,
                pointHoverBackgroundColor: '#f59e0b',
                borderWidth: 2,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                intersect: false,
                mode: 'index',
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#18181b',
                    borderColor: '#232328',
                    borderWidth: 1,
                    titleFont: { family: "'IBM Plex Sans'", size: 12 },
                    bodyFont: { family: "'JetBrains Mono'", size: 12 },
                    callbacks: {
                        label: ctx => 'R$ ' + ctx.parsed.y.toLocaleString('pt-BR', { minimumFractionDigits: 2 })
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

// ── Events ──

document.getElementById('btn-search').addEventListener('click', () => {
    currentFilters = {};
    const search = document.getElementById('search-input').value.trim();
    const state = document.getElementById('filter-state').value;
    const starsVal = document.getElementById('filter-stars').value;
    const chain = document.getElementById('filter-chain').value;

    if (search) currentFilters.search = search;
    if (state) currentFilters.state = state;
    if (starsVal) currentFilters.stars = starsVal;
    if (chain) currentFilters.chain = chain;

    currentPage = 0;
    loadHotels();
});

document.getElementById('search-input').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') document.getElementById('btn-search').click();
});

document.getElementById('btn-prev').addEventListener('click', () => {
    if (currentPage > 0) { currentPage--; loadHotels(); }
});

document.getElementById('btn-next').addEventListener('click', () => {
    currentPage++;
    loadHotels();
});

document.getElementById('btn-close-detail').addEventListener('click', () => {
    document.getElementById('detail-panel').style.display = 'none';
    selectedHotel = null;
});

document.getElementById('btn-date-filter').addEventListener('click', () => {
    if (!selectedHotel) return;
    const dateFrom = document.getElementById('price-date-from').value;
    const dateTo = document.getElementById('price-date-to').value;
    if (!dateFrom || !dateTo) return;
    loadPrices(selectedHotel, dateFrom, dateTo);
});

document.getElementById('btn-date-reset').addEventListener('click', () => {
    if (!selectedHotel) return;
    document.getElementById('price-date-from').value = '';
    document.getElementById('price-date-to').value = '';
    loadPrices(selectedHotel);
});

// ── Init ──

async function init() {
    await Promise.all([
        loadStats(),
        loadCities(),
        loadDistribution(),
        loadFilters(),
    ]);
    await loadHotels();
}

init();
