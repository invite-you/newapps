/**
 * 신규 앱 발견 웹사이트 JavaScript
 * API와 통신하여 앱 목록을 동적으로 표시
 */

// 전역 설정값
const API_BASE_URL = 'http://localhost:3000/api';
let currentPage = 1;
let currentFilters = {
    country: 'all',
    platform: 'all',
    featured: true
};

// 국가 코드 -> 이름 매핑
const COUNTRY_NAMES = {
    'kr': '대한민국',
    'jp': '일본',
    'cn': '중국',
    'tw': '대만',
    'hk': '홍콩',
    'sg': '싱가포르',
    'in': '인도',
    'id': '인도네시아',
    'th': '태국',
    'vn': '베트남',
    'ph': '필리핀',
    'my': '말레이시아',
    'us': '미국',
    'ca': '캐나다',
    'mx': '멕시코',
    'gb': '영국',
    'de': '독일',
    'fr': '프랑스',
    'it': '이탈리아',
    'es': '스페인',
    'nl': '네덜란드',
    'se': '스웨덴',
    'no': '노르웨이',
    'dk': '덴마크',
    'fi': '핀란드',
    'pl': '폴란드',
    'ru': '러시아',
    'au': '호주',
    'nz': '뉴질랜드',
    'br': '브라질',
    'ar': '아르헨티나',
    'cl': '칠레',
    'ae': '아랍에미리트',
    'sa': '사우디아라비아',
    'za': '남아프리카공화국',
    'eg': '이집트'
};

/**
 * 페이지 로드 시 초기화
 */
document.addEventListener('DOMContentLoaded', () => {
    loadStats();
    loadCountries();
    loadApps();

    // 필터 이벤트 리스너
    document.getElementById('applyFilters').addEventListener('click', applyFilters);
    document.getElementById('countryFilter').addEventListener('change', applyFilters);
    document.getElementById('platformFilter').addEventListener('change', applyFilters);
    document.getElementById('featuredFilter').addEventListener('change', applyFilters);
});

/**
 * 통계 정보 로드
 */
async function loadStats() {
    try {
        const response = await fetch(`${API_BASE_URL}/stats`);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        const stats = await response.json();

        document.getElementById('totalApps').textContent = stats.total.toLocaleString();
        document.getElementById('featuredApps').textContent = stats.featured.toLocaleString();
        document.getElementById('countryCount').textContent = stats.byCountry.length;
    } catch (error) {
        console.error('통계 로드 실패:', error);
        // 사용자에게 에러 표시
        document.getElementById('totalApps').textContent = '오류';
        document.getElementById('featuredApps').textContent = '오류';
        document.getElementById('countryCount').textContent = '오류';
    }
}

/**
 * 국가 목록 로드
 */
async function loadCountries() {
    try {
        const response = await fetch(`${API_BASE_URL}/countries`);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        const data = await response.json();

        const countryFilter = document.getElementById('countryFilter');

        data.countries.forEach(country => {
            const option = document.createElement('option');
            option.value = country.country_code;
            option.textContent = `${COUNTRY_NAMES[country.country_code] || country.country_code} (${country.app_count})`;
            countryFilter.appendChild(option);
        });
    } catch (error) {
        console.error('국가 목록 로드 실패:', error);
        alert('국가 목록을 불러오는데 실패했습니다. 페이지를 새로고침해주세요.');
    }
}

/**
 * 앱 목록 로드
 */
async function loadApps(page = 1) {
    const appList = document.getElementById('appList');
    const loading = document.getElementById('loading');
    const noResults = document.getElementById('noResults');

    // 로딩 표시
    appList.innerHTML = '';
    loading.style.display = 'block';
    noResults.style.display = 'none';

    try {
        const params = new URLSearchParams({
            country: currentFilters.country,
            platform: currentFilters.platform,
            featured: currentFilters.featured,
            page: page
        });

        const response = await fetch(`${API_BASE_URL}/apps?${params}`);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        const data = await response.json();

        // currentPage 업데이트
        currentPage = page;

        loading.style.display = 'none';

        if (data.apps.length === 0) {
            noResults.style.display = 'block';
            return;
        }

        // 앱 카드 생성
        data.apps.forEach(app => {
            const card = createAppCard(app);
            appList.appendChild(card);
        });

        // 페이지네이션 생성
        createPagination(data.page, data.totalPages);

    } catch (error) {
        console.error('앱 목록 로드 실패:', error);
        loading.style.display = 'none';
        noResults.style.display = 'block';
        noResults.textContent = '앱 목록을 불러오는데 실패했습니다. 다시 시도해주세요.';
    }
}

/**
 * 앱 카드 생성
 */
function createAppCard(app) {
    const card = document.createElement('div');
    card.className = 'app-card';

    const platformName = app.platform === 'google_play' ? 'Google Play' : 'App Store';
    const countryName = COUNTRY_NAMES[app.country_code] || app.country_code;
    const rating = app.rating ? `⭐ ${app.rating.toFixed(1)}` : 'N/A';
    const ratingCount = app.rating_count ? `(${app.rating_count.toLocaleString()})` : '';

    card.innerHTML = `
        <div class="app-header">
            <img src="${app.icon_url || 'https://via.placeholder.com/60'}" alt="${app.title}" class="app-icon" onerror="this.src='https://via.placeholder.com/60'">
            <div class="app-info">
                <h3>${app.title}</h3>
                <div class="app-developer">${app.developer || 'Unknown'}</div>
            </div>
        </div>
        <div class="app-meta">
            <span class="app-rating">${rating} ${ratingCount}</span>
            <span class="app-platform">${platformName}</span>
            <span class="app-country">${countryName}</span>
        </div>
        <div class="app-score">점수: ${app.score || 0}</div>
        ${app.is_featured ? '<div class="app-featured">주목 앱</div>' : ''}
    `;

    // 클릭 시 앱스토어로 이동
    card.addEventListener('click', () => {
        if (app.url) {
            window.open(app.url, '_blank');
        }
    });

    return card;
}

/**
 * 페이지네이션 생성
 */
function createPagination(currentPage, totalPages) {
    const pagination = document.getElementById('pagination');
    pagination.innerHTML = '';

    if (totalPages <= 1) return;

    // 이전 버튼
    const prevBtn = document.createElement('button');
    prevBtn.textContent = '이전';
    prevBtn.disabled = currentPage === 1;
    prevBtn.addEventListener('click', () => {
        if (currentPage > 1) {
            loadApps(currentPage - 1);
        }
    });
    pagination.appendChild(prevBtn);

    // 페이지 번호 버튼 (최대 5개)
    const startPage = Math.max(1, currentPage - 2);
    const endPage = Math.min(totalPages, startPage + 4);

    for (let i = startPage; i <= endPage; i++) {
        const pageBtn = document.createElement('button');
        pageBtn.textContent = i;
        pageBtn.className = i === currentPage ? 'active' : '';
        pageBtn.addEventListener('click', () => loadApps(i));
        pagination.appendChild(pageBtn);
    }

    // 다음 버튼
    const nextBtn = document.createElement('button');
    nextBtn.textContent = '다음';
    nextBtn.disabled = currentPage === totalPages;
    nextBtn.addEventListener('click', () => {
        if (currentPage < totalPages) {
            loadApps(currentPage + 1);
        }
    });
    pagination.appendChild(nextBtn);
}

/**
 * 필터 적용
 */
function applyFilters() {
    currentFilters = {
        country: document.getElementById('countryFilter').value,
        platform: document.getElementById('platformFilter').value,
        featured: document.getElementById('featuredFilter').checked
    };

    currentPage = 1;
    loadApps(1);
}
