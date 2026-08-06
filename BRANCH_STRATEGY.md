# 分支管理策略

本项目采用三分支策略维护 Fork 版本。

## 分支说明

### main（发布分支）
- **用途**：稳定的可发布版本
- **状态**：始终可用于生产部署
- **更新方式**：从 `develop` 合并
- **Tag**：在此分支打版本标签（如 `v0.7.2-custom`）

### develop（开发分支）
- **用途**：日常开发和功能集成
- **状态**：包含所有待发布的功能
- **更新方式**：从 `upstream-sync` 合并官方更新，从 `feature/*` 合并新功能
- **注意**：可能包含未完成的实验性功能

### upstream-sync（同步分支）
- **用途**：追踪官方仓库最新状态
- **状态**：与官方 `main` 保持同步
- **更新方式**：通过 `scripts/sync-upstream.sh` 自动同步
- **注意**：不要直接在此分支开发

## 工作流程

### 日常开发

```bash
# 1. 基于 develop 创建功能分支
git checkout develop
git checkout -b feature/new-feature

# 2. 开发完成后合并回 develop
git checkout develop
git merge feature/new-feature --no-ff

# 3. 推送到远程
git push origin develop
```

### 同步官方更新（建议每周一次）

```bash
# 方式 1：使用同步脚本（推荐）
./scripts/sync-upstream.sh

# 方式 2：手动同步
git checkout upstream-sync
git pull upstream main
git push origin upstream-sync

# 合并到 develop
git checkout develop
git merge upstream-sync
```

### 发布新版本

```bash
# 1. 确保 develop 稳定
git checkout develop
# 运行测试...

# 2. 合并到 main
git checkout main
git merge develop --no-ff -m "Release v0.7.2-custom"

# 3. 打标签
git tag -a v0.7.2-custom -m "Custom release v0.7.2"

# 4. 推送
git push origin main --tags

# 5. 构建 Docker 镜像（如果配置了 CI/CD）
# GitHub Actions 会自动构建并推送镜像
```

### 处理冲突

当 `upstream-sync` 与 `develop` 有冲突时：

```bash
# 1. 在 develop 分支合并
git checkout develop
git merge upstream-sync

# 2. 解决冲突
# ... 手动解决冲突 ...

# 3. 完成合并
git add .
git commit

# 4. 测试并推送
# ... 运行测试 ...
git push origin develop
```

## 分支保护规则

### main 分支
- ✅ 要求 Pull Request
- ✅ 要求状态检查通过
- ❌ 禁止强制推送
- ❌ 禁止直接提交

### develop 分支
- ✅ 允许直接推送（开发阶段）
- ✅ 允许 Pull Request
- ⚠️ 建议要求审查

### upstream-sync 分支
- ✅ 允许 fast-forward 合并
- ✅ 允许脚本自动推送
- ❌ 不建议手动开发

## 版本命名

自定义版本使用后缀区分：
- 官方版本：`v0.7.1`
- 自定义版本：`v0.7.1-custom.1`, `v0.7.1-custom.2`

## Docker 镜像

- 官方镜像：`wechatopenai/weknora-*`
- 自定义镜像：`jacentsao/weknora-*`（需要配置 CI/CD）

## 注意事项

1. **定期同步**：建议每周同步一次官方更新，避免积累过多冲突
2. **测试优先**：合并 `upstream-sync` 后务必运行完整测试
3. **小步快跑**：功能分支保持小而频繁，便于合并和回滚
4. **文档更新**：重要变更及时更新文档和 CHANGELOG
