# Backend - App Store & Play Store Data Collection

앱 스토어 및 플레이 스토어에서 앱 정보와 리뷰를 수집하는 파이프라인입니다.

---

## 빠른 시작 (서버 셋업 완료 후)

서버 셋업이 완료된 상태에서 새 터미널 세션을 시작할 때 사용합니다.

```bash
# 1. 프로젝트 디렉토리로 이동
cd ~/newapps/backend

# 2. 가상환경 활성화
source .venv/bin/activate

# 3. 환경변수 로드
set -a && source .env && set +a

# 4. 작업 시작 (아래 중 선택)
python -m pytest -v                      # 테스트 실행
python collect_full_pipeline.py          # 단일 실행
python collect_full_pipeline.py --daemon # 데몬 모드
```

**한 줄로 실행:**
```bash
cd ~/newapps/backend && source .venv/bin/activate && set -a && source .env && set +a
```

**서비스 상태 확인:**
```bash
sudo systemctl status collect-pipeline   # 서비스 상태
sudo journalctl -u collect-pipeline -f   # 실시간 로그
```

**Git 업데이트:**
```bash
cd ~/newapps/backend && git pull origin main && source .venv/bin/activate && pip install -r requirements.txt
```

---

## 서버 이전 가이드

### 1. 시스템 요구사항

```bash
# Ubuntu 22.04+ 권장
# Python 3.12+
# PostgreSQL 15+
```

### 2. SSH 키 생성 및 GitHub 설정

```bash
# SSH 키 생성 (Enter 연타로 기본값 사용)
ssh-keygen -t ed25519 -C "your-email@example.com"

# 공개키 출력 (이 값을 GitHub에 등록)
cat ~/.ssh/id_ed25519.pub
```

**GitHub Deploy Key 등록:**
1. GitHub 저장소 → Settings → Deploy keys → Add deploy key
2. Title: `서버명 (예: aws-ap-southeast-2)`
3. Key: 위에서 출력한 공개키 붙여넣기
4. "Allow write access" 체크
5. Add key 클릭

```bash
# GitHub 연결 테스트
ssh -T git@github.com
# "Hi invite-you/newapps!" 메시지 확인
```

### 3. 저장소 클론

```bash
# 작업 디렉토리 생성
mkdir -p ~/newapps
cd ~/newapps

# 저장소 클론
git clone git@github.com:invite-you/newapps.git .

# backend 디렉토리로 이동
cd backend
```

### 4. Python 환경 설정

```bash
# Python 버전 확인
python3 --version  # 3.12+ 필요

# 가상환경 생성
python3 -m venv .venv

# 가상환경 활성화
source .venv/bin/activate

# 의존성 설치
pip install --upgrade pip
pip install -r requirements.txt

# 추가 의존성 (필요시)
pip install pytest pytest-cov
```

### 5. 환경 변수 설정

```bash
# .env 파일 생성
cat > .env << 'EOF'
# Database
APP_DETAILS_DB_HOST=localhost
APP_DETAILS_DB_PORT=5432
APP_DETAILS_DB_NAME=app_details
APP_DETAILS_DB_USER=app_details
APP_DETAILS_DB_PASSWORD=your_secure_password_here

# Collection Settings (선택사항)
APP_REVIEWS_MAX_PER_RUN=50000
APP_STORE_REVIEW_TIMEOUT=60
APP_REVIEWS_LOG_RETENTION_DAYS=365
EOF

# 권한 설정 (보안)
chmod 600 .env
```

### 6. 데이터베이스 설정

```bash
# PostgreSQL 설치 (Ubuntu)
sudo apt update
sudo apt install postgresql postgresql-contrib

# PostgreSQL 시작
sudo systemctl start postgresql
sudo systemctl enable postgresql

# 데이터베이스 및 사용자 생성
sudo -u postgres psql << 'EOF'
CREATE USER app_details WITH PASSWORD 'your_secure_password_here';
CREATE DATABASE app_details OWNER app_details;
GRANT ALL PRIVILEGES ON DATABASE app_details TO app_details;
\c app_details
GRANT ALL ON SCHEMA public TO app_details;
EOF
```

### 7. 테스트 실행

```bash
# 가상환경 활성화 확인
source .venv/bin/activate

# 테스트 실행
python -m pytest -v

# 커버리지 포함 테스트
python -m pytest --cov=. --cov-report=term-missing
```

### 8. Systemd 서비스 설정

```bash
# 서비스 파일 복사
sudo cp collect-pipeline.service /etc/systemd/system/

# 서비스 파일 편집 (경로 확인 및 .env 주석 해제)
sudo nano /etc/systemd/system/collect-pipeline.service
```

서비스 파일에서 수정할 부분:
```ini
# EnvironmentFile 주석 해제
EnvironmentFile=/home/ubuntu/newapps/backend/.env
```

```bash
# systemd 리로드 및 서비스 시작
sudo systemctl daemon-reload
sudo systemctl enable collect-pipeline
sudo systemctl start collect-pipeline

# 상태 확인
sudo systemctl status collect-pipeline

# 로그 확인
sudo journalctl -u collect-pipeline -f
```

### 9. 수동 실행 (테스트용)

```bash
# 단일 실행
source .venv/bin/activate
python collect_full_pipeline.py

# 데몬 모드 (무한 루프)
python collect_full_pipeline.py --daemon

# 인터벌 지정 (초 단위)
python collect_full_pipeline.py --daemon --interval 3600
```

---

## Claude Code & Codex Skills 동기화

이 프로젝트는 Claude Code와 OpenAI Codex 모두에서 동일한 skills을 사용합니다.

### 현재 등록된 Skills

| 타입 | 이름 | 설명 | 적용 시점 |
|------|------|------|-----------|
| **Rules** | `coding-style` | Python 코딩 스타일 가이드 | 항상 자동 적용 |
| **Rules** | `testing` | pytest 테스트 작성 규칙 | 항상 자동 적용 |
| **Rules** | `security` | 보안 관련 규칙 (secrets, SQL injection 등) | 항상 자동 적용 |
| **Agents** | `code-reviewer` | 코드 리뷰 에이전트 | Task 도구로 호출 |
| **Agents** | `planner` | 구현 계획 작성 에이전트 | Task 도구로 호출 |
| **Agents** | `build-error-resolver` | 빌드 에러 해결 에이전트 | Task 도구로 호출 |
| **Skills** | `tdd-workflow` | TDD 워크플로우 | `/tdd-workflow`로 호출 |
| **Skills** | `postgres-best-practices` | PostgreSQL 최적화 패턴 | `/postgres-best-practices`로 호출 |
| **Skills** | `logging-best-practices` | 구조화된 로깅 패턴 | `/logging-best-practices`로 호출 |
| **Skills** | `python-scraper-patterns` | 웹 스크래핑 베스트 프랙티스 | `/python-scraper-patterns`로 호출 |
| **Skills** | `systematic-debugging` | 체계적 디버깅 방법론 | `/systematic-debugging`로 호출 |
| **Skills** | `fastapi-backend` | FastAPI 백엔드 패턴 | `/fastapi-backend`로 호출 |

### 타입별 차이점

| 타입 | 위치 | 적용 방식 | 용도 |
|------|------|-----------|------|
| **Rules** | `.claude/rules/*.md` | 모든 대화에 자동 적용 | 코딩 스타일, 보안 규칙 등 |
| **Agents** | `.claude/agents/*.md` | Task 도구의 subagent로 호출 | 특정 작업 수행 (리뷰, 계획 등) |
| **Skills** | `.claude/skills/*/SKILL.md` | `/skill-name`으로 수동 호출 | 워크플로우, 베스트 프랙티스 |

### 디렉토리 구조

```
.claude/                          # Claude Code 설정
├── agents/                       # 에이전트 정의 (Task 도구로 호출)
│   ├── code-reviewer.md
│   ├── planner.md
│   └── build-error-resolver.md
├── rules/                        # 프로젝트 규칙 (항상 자동 적용)
│   ├── coding-style.md
│   ├── testing.md
│   └── security.md
├── skills/                       # Skills (/명령어로 호출)
│   ├── tdd-workflow/SKILL.md
│   ├── postgres-best-practices/SKILL.md
│   ├── logging-best-practices/SKILL.md
│   ├── python-scraper-patterns/SKILL.md
│   ├── systematic-debugging/SKILL.md
│   └── fastapi-backend/SKILL.md
├── settings.json                 # 공유 설정 (git tracked)
└── settings.local.json           # 로컬 설정 (git ignored)

.codex/                           # Codex 설정
└── skills/                       # 모든 설정이 skills로 통합됨
    ├── tdd-workflow/SKILL.md
    ├── postgres-best-practices/SKILL.md
    ├── ... (skills)
    ├── coding-style/SKILL.md     # .claude/rules/에서 변환
    ├── testing/SKILL.md
    ├── security/SKILL.md
    ├── code-reviewer/SKILL.md    # .claude/agents/에서 변환
    ├── planner/SKILL.md
    └── build-error-resolver/SKILL.md
```

### Skills 동기화 스크립트

동일한 skills을 양쪽에서 사용하려면 아래 스크립트를 실행하세요:

```bash
#!/bin/bash
# scripts/sync-skills.sh

set -e
cd "$(dirname "$0")/.."

echo "=== Syncing skills between .claude and .codex ==="

# 1. .claude/skills -> .codex/skills 동기화
echo "Syncing .claude/skills to .codex/skills..."
for skill_dir in .claude/skills/*/; do
    skill_name=$(basename "$skill_dir")
    mkdir -p ".codex/skills/$skill_name"
    cp -r "$skill_dir"* ".codex/skills/$skill_name/"
    echo "  - $skill_name"
done

# 2. .claude/rules -> .codex/skills 변환
echo "Converting .claude/rules to .codex/skills..."
for rule_file in .claude/rules/*.md; do
    rule_name=$(basename "$rule_file" .md)
    skill_dir=".codex/skills/$rule_name"
    mkdir -p "$skill_dir"

    # SKILL.md 형식으로 변환
    {
        echo "---"
        echo "name: $rule_name"
        echo "description: $(head -1 "$rule_file" | sed 's/^# //')"
        echo "---"
        echo ""
        cat "$rule_file"
    } > "$skill_dir/SKILL.md"
    echo "  - $rule_name (from rules)"
done

# 3. .claude/agents -> .codex/skills 변환
echo "Converting .claude/agents to .codex/skills..."
for agent_file in .claude/agents/*.md; do
    agent_name=$(basename "$agent_file" .md)
    skill_dir=".codex/skills/$agent_name"
    mkdir -p "$skill_dir"

    # SKILL.md 형식으로 변환 (frontmatter 추출 또는 생성)
    if grep -q "^---" "$agent_file"; then
        cp "$agent_file" "$skill_dir/SKILL.md"
    else
        {
            echo "---"
            echo "name: $agent_name"
            echo "description: $(head -1 "$agent_file" | sed 's/^# //')"
            echo "---"
            echo ""
            cat "$agent_file"
        } > "$skill_dir/SKILL.md"
    fi
    echo "  - $agent_name (from agents)"
done

echo "=== Sync complete ==="
```

스크립트 실행:
```bash
# 스크립트 생성
mkdir -p scripts
# 위 내용을 scripts/sync-skills.sh에 저장

# 실행 권한 부여
chmod +x scripts/sync-skills.sh

# 실행
./scripts/sync-skills.sh
```

### 새 Skill 추가 방법

#### 1. Rule 추가 (항상 자동 적용되는 규칙)

```bash
# Rule 파일 생성 (frontmatter 불필요)
cat > .claude/rules/my-new-rule.md << 'EOF'
# My New Rule

## 원칙
- 규칙 1
- 규칙 2

## 예시
```python
# 좋은 예시
def good_example():
    pass
```
EOF

# Codex에 동기화
./scripts/sync-skills.sh

# 커밋
git add .claude/rules/my-new-rule.md .codex/skills/my-new-rule/
git commit -m "feat: add my-new-rule to rules"
git push
```

#### 2. Agent 추가 (Task 도구로 호출되는 에이전트)

```bash
# Agent 파일 생성 (frontmatter 필요)
cat > .claude/agents/my-new-agent.md << 'EOF'
---
name: my-new-agent
description: 에이전트가 하는 일 설명
tools:
  - Read
  - Grep
  - Glob
---

# My New Agent

이 에이전트는 특정 작업을 수행합니다.

## 작업 절차
1. 첫 번째 단계
2. 두 번째 단계

## 주의사항
- 주의할 점
EOF

# Codex에 동기화
./scripts/sync-skills.sh

# 커밋
git add .claude/agents/my-new-agent.md .codex/skills/my-new-agent/
git commit -m "feat: add my-new-agent"
git push
```

#### 3. Skill 추가 (/명령어로 호출되는 워크플로우)

```bash
# Skill 디렉토리 및 파일 생성
mkdir -p .claude/skills/my-new-skill
cat > .claude/skills/my-new-skill/SKILL.md << 'EOF'
---
name: my-new-skill
description: 이 skill이 하는 일 설명
---

# My New Skill

이 skill은 `/my-new-skill` 명령어로 호출됩니다.

## 사용 시점
- 이럴 때 사용

## 워크플로우
1. 첫 번째 단계
2. 두 번째 단계
3. 세 번째 단계

## 체크리스트
- [ ] 확인 항목 1
- [ ] 확인 항목 2
EOF

# Codex에 동기화
./scripts/sync-skills.sh

# 커밋
git add .claude/skills/my-new-skill/ .codex/skills/my-new-skill/
git commit -m "feat: add my-new-skill"
git push
```

### 동기화 및 커밋 (전체)

```bash
# 모든 skills 동기화
./scripts/sync-skills.sh

# 변경사항 확인
git status

# 전체 커밋
git add .claude/ .codex/
git commit -m "feat: update skills"
git push
```

### 수동 동기화 (단일 파일)

```bash
# Skill 하나만 동기화
cp -r .claude/skills/tdd-workflow/* .codex/skills/tdd-workflow/

# Rule을 Codex skill로 변환
mkdir -p .codex/skills/coding-style
cat > .codex/skills/coding-style/SKILL.md << EOF
---
name: coding-style
description: Python coding style guide
---

$(cat .claude/rules/coding-style.md)
EOF

# Agent를 Codex skill로 변환
mkdir -p .codex/skills/code-reviewer
cp .claude/agents/code-reviewer.md .codex/skills/code-reviewer/SKILL.md
```

---

## 주요 파일 구조

```
backend/
├── collect_full_pipeline.py      # 메인 파이프라인 (데몬 지원)
├── collect_app_details.py        # 앱 상세정보 수집
├── collect_sitemaps.py           # 사이트맵 수집
├── collect-pipeline.service      # systemd 서비스 파일
├── requirements.txt              # Python 의존성
├── pytest.ini                    # pytest 설정
│
├── core/                         # 핵심 모듈
│   ├── ip_manager.py             # IP 관리 및 로테이션
│   ├── http_client.py            # HTTP 클라이언트
│   └── review_collection_integration.py
│
├── database/                     # 데이터베이스 모듈
│   ├── app_details_db.py         # 앱 상세 DB
│   └── review_collection_db.py   # 리뷰 수집 상태 DB
│
├── scrapers/                     # 스크래퍼
│   ├── app_store_reviews_collector.py
│   └── play_store_reviews_collector.py
│
├── utils/                        # 유틸리티
│   ├── logger.py
│   ├── network_binding.py
│   └── error_tracker.py
│
└── tests/                        # 테스트
    ├── test_ip_manager.py
    ├── test_ip_rotation.py
    └── ...
```

---

## 트러블슈팅

### SSH 연결 실패
```bash
# SSH 에이전트 시작
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519

# 권한 확인
chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_ed25519
chmod 644 ~/.ssh/id_ed25519.pub
```

### 가상환경 문제
```bash
# 가상환경 재생성
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 데이터베이스 연결 실패
```bash
# PostgreSQL 상태 확인
sudo systemctl status postgresql

# 연결 테스트
psql -h localhost -U app_details -d app_details -c "SELECT 1;"

# pg_hba.conf 확인 (로컬 연결 허용)
sudo cat /etc/postgresql/*/main/pg_hba.conf | grep -v "^#"
```

### 서비스 로그 확인
```bash
# 실시간 로그
sudo journalctl -u collect-pipeline -f

# 최근 100줄
sudo journalctl -u collect-pipeline -n 100

# 특정 시간 이후
sudo journalctl -u collect-pipeline --since "1 hour ago"
```

---

## 빠른 참조

```bash
# 서비스 관리
sudo systemctl start collect-pipeline
sudo systemctl stop collect-pipeline
sudo systemctl restart collect-pipeline
sudo systemctl status collect-pipeline

# 테스트
source .venv/bin/activate
python -m pytest -v

# Skills 동기화
./scripts/sync-skills.sh

# Git 업데이트
git pull origin main
```
