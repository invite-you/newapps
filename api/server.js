/**
 * 신규 앱 발견 웹사이트 API 서버
 * Express + SQLite로 간결하게 구현
 */

const express = require('express');
const cors = require('cors');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// 전역 설정값 (AGENT.MD 지침 5번)
const PORT = 3000;
const DB_PATH = path.join(__dirname, '../database/apps.db');
const SITEMAP_DB_PATH = path.join(__dirname, '../database/sitemap_tracking.db');
const APPS_PER_PAGE = 20;

const app = express();

// 미들웨어
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../frontend')));

// 데이터베이스 연결
const db = new sqlite3.Database(DB_PATH, (err) => {
    if (err) {
        console.error('앱 데이터베이스 연결 실패:', err);
    } else {
        console.log('앱 데이터베이스 연결 성공');
    }
});

// Sitemap/메트릭 데이터베이스 연결 (시계열 분석용)
const sitemapDb = new sqlite3.Database(SITEMAP_DB_PATH, (err) => {
    if (err) {
        console.error('Sitemap 데이터베이스 연결 실패:', err);
    } else {
        console.log('Sitemap 데이터베이스 연결 성공');
    }
});

/**
 * API: 국가 목록 조회
 * GET /api/countries
 */
app.get('/api/countries', (req, res) => {
    const query = `
        SELECT DISTINCT country_code, COUNT(*) as app_count
        FROM apps
        GROUP BY country_code
        ORDER BY app_count DESC
    `;

    db.all(query, [], (err, rows) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }
        res.json({ countries: rows });
    });
});

/**
 * API: 앱 목록 조회
 * GET /api/apps?country=kr&platform=all&featured=true&page=1
 */
app.get('/api/apps', (req, res) => {
    const country = req.query.country || 'all';
    const platform = req.query.platform || 'all';
    const featured = req.query.featured === 'true';
    const page = parseInt(req.query.page) || 1;
    const offset = (page - 1) * APPS_PER_PAGE;

    // WHERE 조건 구성
    let whereClause = 'WHERE 1=1';
    const filterParams = [];

    if (country !== 'all') {
        whereClause += ' AND country_code = ?';
        filterParams.push(country);
    }

    if (platform !== 'all') {
        whereClause += ' AND platform = ?';
        filterParams.push(platform);
    }

    if (featured) {
        whereClause += ' AND is_featured = 1';
    }

    // 쿼리 구성
    const countQuery = `SELECT COUNT(*) as total FROM apps ${whereClause}`;
    const dataQuery = `SELECT * FROM apps ${whereClause} ORDER BY score DESC, created_at DESC LIMIT ? OFFSET ?`;
    const dataParams = [...filterParams, APPS_PER_PAGE, offset];

    db.get(countQuery, filterParams, (err, countRow) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }

        db.all(dataQuery, dataParams, (err, rows) => {
            if (err) {
                res.status(500).json({ error: err.message });
                return;
            }

            res.json({
                apps: rows,
                total: countRow.total,
                page: page,
                totalPages: Math.ceil(countRow.total / APPS_PER_PAGE)
            });
        });
    });
});

/**
 * API: 앱 상세 정보 조회
 * GET /api/apps/:id
 */
app.get('/api/apps/:id', (req, res) => {
    const query = 'SELECT * FROM apps WHERE id = ?';

    db.get(query, [req.params.id], (err, row) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }

        if (!row) {
            res.status(404).json({ error: '앱을 찾을 수 없습니다' });
            return;
        }

        res.json(row);
    });
});

/**
 * API: 통계 정보 조회
 * GET /api/stats
 */
app.get('/api/stats', async (req, res) => {
    try {
        const queries = {
            total: 'SELECT COUNT(*) as count FROM apps',
            featured: 'SELECT COUNT(*) as count FROM apps WHERE is_featured = 1',
            byPlatform: 'SELECT platform, COUNT(*) as count FROM apps GROUP BY platform',
            byCountry: 'SELECT country_code, COUNT(*) as count FROM apps GROUP BY country_code ORDER BY count DESC LIMIT 10'
        };

        // Promise 래퍼 함수
        const dbGet = (query) => new Promise((resolve, reject) => {
            db.get(query, [], (err, row) => {
                if (err) reject(err);
                else resolve(row);
            });
        });

        const dbAll = (query) => new Promise((resolve, reject) => {
            db.all(query, [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        // 병렬 실행
        const [totalRow, featuredRow, byPlatformRows, byCountryRows] = await Promise.all([
            dbGet(queries.total),
            dbGet(queries.featured),
            dbAll(queries.byPlatform),
            dbAll(queries.byCountry)
        ]);

        res.json({
            total: totalRow ? totalRow.count : 0,
            featured: featuredRow ? featuredRow.count : 0,
            byPlatform: byPlatformRows || [],
            byCountry: byCountryRows || []
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ============ 시계열 분석 API ============

/**
 * API: 앱 메트릭 시계열 데이터 조회
 * GET /api/apps/:appId/metrics?platform=google_play&country=kr&days=30
 */
app.get('/api/apps/:appId/metrics', (req, res) => {
    const { appId } = req.params;
    const platform = req.query.platform || 'google_play';
    const country = req.query.country;
    const days = parseInt(req.query.days) || 30;

    let query = `
        SELECT recorded_date, country_code,
               rating, rating_count, reviews_count,
               installs_min, installs_exact,
               chart_position, score, price, version
        FROM app_metrics_history
        WHERE app_id = ? AND platform = ?
          AND recorded_date >= date('now', ?)
    `;
    const params = [appId, platform, `-${days} days`];

    if (country) {
        query += ' AND country_code = ?';
        params.push(country);
    }

    query += ' ORDER BY recorded_date ASC, country_code';

    sitemapDb.all(query, params, (err, rows) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }

        res.json({
            app_id: appId,
            platform: platform,
            days: days,
            data: rows
        });
    });
});

/**
 * API: 앱 메트릭 변화량 조회 (기간 비교)
 * GET /api/apps/:appId/metrics/changes?platform=google_play&country=kr&days=7
 */
app.get('/api/apps/:appId/metrics/changes', async (req, res) => {
    const { appId } = req.params;
    const platform = req.query.platform || 'google_play';
    const country = req.query.country || 'us';
    const days = parseInt(req.query.days) || 7;

    const dbGet = (query, params) => new Promise((resolve, reject) => {
        sitemapDb.get(query, params, (err, row) => {
            if (err) reject(err);
            else resolve(row);
        });
    });

    try {
        // 현재 데이터 (가장 최근)
        const currentQuery = `
            SELECT rating, rating_count, reviews_count, installs_min, installs_exact,
                   chart_position, score, price, recorded_date
            FROM app_metrics_history
            WHERE app_id = ? AND platform = ? AND country_code = ?
            ORDER BY recorded_date DESC
            LIMIT 1
        `;
        const current = await dbGet(currentQuery, [appId, platform, country]);

        // 과거 데이터 (days일 전)
        const pastQuery = `
            SELECT rating, rating_count, reviews_count, installs_min, installs_exact,
                   chart_position, score, price, recorded_date
            FROM app_metrics_history
            WHERE app_id = ? AND platform = ? AND country_code = ?
              AND recorded_date <= date('now', ?)
            ORDER BY recorded_date DESC
            LIMIT 1
        `;
        const past = await dbGet(pastQuery, [appId, platform, country, `-${days} days`]);

        if (!current || !past) {
            res.json({
                app_id: appId,
                platform: platform,
                country: country,
                days: days,
                changes: {},
                message: '비교할 데이터가 충분하지 않습니다'
            });
            return;
        }

        // 변화량 계산
        const numericFields = ['rating', 'rating_count', 'reviews_count', 'installs_min',
                              'installs_exact', 'chart_position', 'score', 'price'];
        const changes = {};

        for (const field of numericFields) {
            const currVal = current[field];
            const pastVal = past[field];

            if (currVal !== null && pastVal !== null) {
                const diff = currVal - pastVal;
                const pctChange = pastVal !== 0 ? ((currVal - pastVal) / pastVal * 100) : null;
                changes[field] = {
                    current: currVal,
                    past: pastVal,
                    diff: diff,
                    pct_change: pctChange ? Math.round(pctChange * 100) / 100 : null
                };
            }
        }

        res.json({
            app_id: appId,
            platform: platform,
            country: country,
            days: days,
            current_date: current.recorded_date,
            past_date: past.recorded_date,
            changes: changes
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

/**
 * API: 성장 상위 앱 조회
 * GET /api/metrics/top-growing?platform=google_play&country=kr&metric=rating_count&days=7&limit=50
 */
app.get('/api/metrics/top-growing', (req, res) => {
    const platform = req.query.platform;
    const country = req.query.country;
    const metric = req.query.metric || 'rating_count';
    const days = parseInt(req.query.days) || 7;
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);

    // 유효한 메트릭 확인
    const validMetrics = ['rating', 'rating_count', 'reviews_count', 'installs_min',
                         'installs_exact', 'score'];
    const safeMetric = validMetrics.includes(metric) ? metric : 'rating_count';

    let whereClause = `recorded_date >= date('now', '-${days} days')`;
    const params = [];

    if (platform) {
        whereClause += ' AND platform = ?';
        params.push(platform);
    }
    if (country) {
        whereClause += ' AND country_code = ?';
        params.push(country);
    }

    const query = `
        WITH date_range AS (
            SELECT
                app_id, platform, country_code,
                MIN(recorded_date) as first_date,
                MAX(recorded_date) as last_date
            FROM app_metrics_history
            WHERE ${whereClause}
            GROUP BY app_id, platform, country_code
            HAVING first_date != last_date
        ),
        first_metrics AS (
            SELECT m.app_id, m.platform, m.country_code, m.${safeMetric} as first_value
            FROM app_metrics_history m
            JOIN date_range d ON m.app_id = d.app_id
                AND m.platform = d.platform
                AND m.country_code = d.country_code
                AND m.recorded_date = d.first_date
        ),
        last_metrics AS (
            SELECT m.app_id, m.platform, m.country_code, m.${safeMetric} as last_value
            FROM app_metrics_history m
            JOIN date_range d ON m.app_id = d.app_id
                AND m.platform = d.platform
                AND m.country_code = d.country_code
                AND m.recorded_date = d.last_date
        )
        SELECT
            f.app_id, f.platform, f.country_code,
            f.first_value,
            l.last_value,
            (l.last_value - f.first_value) as diff,
            CASE WHEN f.first_value > 0
                THEN ROUND((l.last_value - f.first_value) * 100.0 / f.first_value, 2)
                ELSE NULL
            END as growth_pct
        FROM first_metrics f
        JOIN last_metrics l ON f.app_id = l.app_id
            AND f.platform = l.platform
            AND f.country_code = l.country_code
        WHERE l.last_value > f.first_value
        ORDER BY diff DESC
        LIMIT ?
    `;

    sitemapDb.all(query, [...params, limit], (err, rows) => {
        if (err) {
            res.status(500).json({ error: err.message });
            return;
        }

        res.json({
            metric: safeMetric,
            days: days,
            results: rows
        });
    });
});

/**
 * API: 메트릭 통계 조회
 * GET /api/metrics/stats?platform=google_play&country=kr&days=30
 */
app.get('/api/metrics/stats', async (req, res) => {
    const platform = req.query.platform;
    const country = req.query.country;
    const days = parseInt(req.query.days) || 30;

    const dbGet = (query, params) => new Promise((resolve, reject) => {
        sitemapDb.get(query, params, (err, row) => {
            if (err) reject(err);
            else resolve(row);
        });
    });

    const dbAll = (query, params) => new Promise((resolve, reject) => {
        sitemapDb.all(query, params, (err, rows) => {
            if (err) reject(err);
            else resolve(rows);
        });
    });

    try {
        let whereClause = `recorded_date >= date('now', '-${days} days')`;
        const params = [];

        if (platform) {
            whereClause += ' AND platform = ?';
            params.push(platform);
        }
        if (country) {
            whereClause += ' AND country_code = ?';
            params.push(country);
        }

        // 기본 통계
        const statsQuery = `
            SELECT
                COUNT(DISTINCT app_id || platform || country_code) as unique_apps,
                COUNT(*) as total_records,
                COUNT(DISTINCT recorded_date) as days_recorded,
                MIN(recorded_date) as first_date,
                MAX(recorded_date) as last_date
            FROM app_metrics_history
            WHERE ${whereClause}
        `;
        const stats = await dbGet(statsQuery, params);

        // 플랫폼별 통계
        const platformQuery = `
            SELECT
                platform,
                COUNT(DISTINCT app_id) as app_count,
                AVG(rating) as avg_rating,
                AVG(rating_count) as avg_rating_count
            FROM app_metrics_history
            WHERE ${whereClause}
            GROUP BY platform
        `;
        const byPlatform = await dbAll(platformQuery, params);

        res.json({
            ...stats,
            by_platform: byPlatform
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 서버 시작
app.listen(PORT, () => {
    console.log(`\n${'='.repeat(50)}`);
    console.log(`API 서버가 시작되었습니다!`);
    console.log(`포트: ${PORT}`);
    console.log(`웹사이트: http://localhost:${PORT}`);
    console.log(`${'='.repeat(50)}\n`);
});

// 종료 시 데이터베이스 연결 해제
process.on('SIGINT', () => {
    db.close((err) => {
        if (err) {
            console.error('앱 DB 연결 해제 오류:', err.message);
        } else {
            console.log('앱 데이터베이스 연결 해제');
        }

        sitemapDb.close((err) => {
            if (err) {
                console.error('Sitemap DB 연결 해제 오류:', err.message);
            } else {
                console.log('Sitemap 데이터베이스 연결 해제');
            }
            process.exit(0);
        });
    });
});
