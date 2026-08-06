#!/bin/bash
# 同步上游官方仓库更新
# 用法: ./scripts/sync-upstream.sh

set -e

echo "=== 同步上游官方仓库 ==="

# 1. 获取官方更新
echo -e "\n[1/4] 获取官方仓库更新..."
git fetch upstream

# 2. 更新 upstream-sync 分支
echo -e "\n[2/4] 更新 upstream-sync 分支..."
git checkout upstream-sync

# 检查是否有新的提交
LOCAL=$(git rev-parse upstream-sync)
REMOTE=$(git rev-parse upstream/main)

if [ "$LOCAL" = "$REMOTE" ]; then
    echo "✓ upstream-sync 已是最新，无需更新"
else
    git merge upstream/main --ff-only
    git push origin upstream-sync
    echo "✓ upstream-sync 已更新并推送"
fi

# 3. 显示待合并的提交
echo -e "\n[3/4] 各分支与 upstream-sync 的差异..."

echo -e "\n📋 develop 分支落后的提交："
BEHIND=$(git rev-list --left-right --count develop...upstream-sync | awk '{print $2}')
if [ "$BEHIND" -gt 0 ]; then
    echo "  落后 $BEHIND 个提交"
    git log --oneline develop..upstream-sync | head -10
else
    echo "  ✓ 已是最新"
fi

echo -e "\n📋 main 分支落后的提交："
BEHIND=$(git rev-list --left-right --count main...upstream-sync | awk '{print $2}')
if [ "$BEHIND" -gt 0 ]; then
    echo "  落后 $BEHIND 个提交"
    git log --oneline main..upstream-sync | head -10
else
    echo "  ✓ 已是最新"
fi

# 4. 提示下一步
echo -e "\n[4/4] 下一步操作建议："
echo ""
echo "合并到开发分支："
echo "  git checkout develop"
echo "  git merge upstream-sync"
echo ""
echo "准备发布："
echo "  git checkout main"
echo "  git merge develop --no-ff"
echo "  git tag v0.x.x-custom"
echo "  git push origin main --tags"
echo ""

# 切回原来的分支
git checkout - >/dev/null 2>&1 || git checkout main
