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
const DB_PATH = path.join(__dirname, '../data/apps.db');
const APPS_PER_PAGE = 20;

const app = express();

// 미들웨어
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../frontend')));

// 데이터베이스 연결
const db = new sqlite3.Database(DB_PATH, (err) => {
    if (err) {
        console.error('데이터베이스 연결 실패:', err);
    } else {
        console.log('데이터베이스 연결 성공');
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
            console.error(err.message);
        }
        console.log('데이터베이스 연결 해제');
        process.exit(0);
    });
});
