#!/bin/bash
# Sync skills between .claude and .codex directories
# Usage: ./scripts/sync-skills.sh

set -e
cd "$(dirname "$0")/.."

echo "=== Syncing skills between .claude and .codex ==="
echo "Working directory: $(pwd)"
echo ""

# Ensure directories exist
mkdir -p .codex/skills

# 1. .claude/skills -> .codex/skills 동기화
if [ -d ".claude/skills" ]; then
    echo "[1/3] Syncing .claude/skills to .codex/skills..."
    for skill_dir in .claude/skills/*/; do
        if [ -d "$skill_dir" ]; then
            skill_name=$(basename "$skill_dir")
            mkdir -p ".codex/skills/$skill_name"
            cp -r "$skill_dir"* ".codex/skills/$skill_name/" 2>/dev/null || true
            echo "  + $skill_name"
        fi
    done
else
    echo "[1/3] No .claude/skills directory found, skipping..."
fi
echo ""

# 2. .claude/rules -> .codex/skills 변환
if [ -d ".claude/rules" ]; then
    echo "[2/3] Converting .claude/rules to .codex/skills..."
    for rule_file in .claude/rules/*.md; do
        if [ -f "$rule_file" ]; then
            rule_name=$(basename "$rule_file" .md)
            skill_dir=".codex/skills/$rule_name"
            mkdir -p "$skill_dir"

            # 첫 줄에서 제목 추출 (# 제거)
            title=$(head -1 "$rule_file" | sed 's/^#\s*//')

            # SKILL.md 형식으로 변환
            {
                echo "---"
                echo "name: $rule_name"
                echo "description: $title"
                echo "---"
                echo ""
                cat "$rule_file"
            } > "$skill_dir/SKILL.md"
            echo "  + $rule_name (from rules)"
        fi
    done
else
    echo "[2/3] No .claude/rules directory found, skipping..."
fi
echo ""

# 3. .claude/agents -> .codex/skills 변환
if [ -d ".claude/agents" ]; then
    echo "[3/3] Converting .claude/agents to .codex/skills..."
    for agent_file in .claude/agents/*.md; do
        if [ -f "$agent_file" ]; then
            agent_name=$(basename "$agent_file" .md)
            skill_dir=".codex/skills/$agent_name"
            mkdir -p "$skill_dir"

            # 이미 frontmatter가 있는지 확인
            if head -1 "$agent_file" | grep -q "^---"; then
                # frontmatter가 있으면 그대로 복사
                cp "$agent_file" "$skill_dir/SKILL.md"
            else
                # frontmatter가 없으면 생성
                title=$(head -1 "$agent_file" | sed 's/^#\s*//')
                {
                    echo "---"
                    echo "name: $agent_name"
                    echo "description: $title"
                    echo "---"
                    echo ""
                    cat "$agent_file"
                } > "$skill_dir/SKILL.md"
            fi
            echo "  + $agent_name (from agents)"
        fi
    done
else
    echo "[3/3] No .claude/agents directory found, skipping..."
fi
echo ""

echo "=== Sync complete ==="
echo ""
echo "Codex skills now available:"
ls -1 .codex/skills/ 2>/dev/null | sed 's/^/  - /'
