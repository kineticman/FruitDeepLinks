# GitHub Repository Setup Checklist

## Initial Repository Setup

### 1. Create Repository on GitHub
- [x] Repository created: https://github.com/kineticman/FruitDeepLinks
- [ ] Set as **Private** (already done based on your mention)
- [ ] Add description: "Universal Sports Aggregator for Channels DVR - Unified EPG with deeplinks to 18+ streaming services"
- [ ] Add topics/tags: `channels-dvr`, `sports`, `streaming`, `epg`, `deeplinks`, `apple-tv`, `docker`

### 2. Upload Essential Files

**Root Directory:**
```bash
# Navigate to your local project
cd C:\projects\FruitDeepLinks

# Copy files from outputs
copy C:\path\to\outputs\README.md .
copy C:\path\to\outputs\LICENSE .
copy C:\path\to\outputs\.gitignore .
copy C:\path\to\outputs\requirements.txt .
copy C:\path\to\outputs\CONTRIBUTING.md .

# Initialize git (if not already done)
git init
git remote add origin https://github.com/kineticman/FruitDeepLinks.git
```

**Create docs/ folder:**
```bash
mkdir docs
copy C:\path\to\outputs\SERVICE_CATALOG.md docs\
copy C:\path\to\outputs\PHASE4_GUIDE.md docs\
copy C:\path\to\outputs\MULTI_PUNCHOUT_ARCHITECTURE.md docs\
```

### 3. Repository Structure

Ensure your repo has this structure:

```
FruitDeepLinks/
├── .github/
│   ├── ISSUE_TEMPLATE/
│   │   ├── bug_report.md
│   │   └── feature_request.md
│   └── workflows/
│       └── docker-build.yml (optional CI/CD)
├── bin/                          # Your Python scripts
├── docs/                         # Documentation
│   ├── SERVICE_CATALOG.md
│   ├── INSTALLATION.md
│   └── TROUBLESHOOTING.md
├── data/                         # Database (in .gitignore)
├── out/                          # Generated files (in .gitignore)
├── logs/                         # Logs (in .gitignore)
├── .env.example                  # Template environment file
├── .gitignore
├── CONTRIBUTING.md
├── docker-compose.yml
├── Dockerfile
├── LICENSE
├── README.md
└── requirements.txt
```

### 4. Create .env.example

```bash
# Copy your .env but remove sensitive values
copy .env .env.example

# Then edit .env.example to have placeholder values:
# TZ=America/New_York
# SERVER_URL=http://YOUR_SERVER_IP:6655
# CHANNELS_DVR_IP=YOUR_CHANNELS_DVR_IP
# etc.
```

### 5. Initial Commit

```bash
# Stage all files
git add .

# Initial commit
git commit -m "Initial commit: FruitDeepLinks v1.0

- Universal sports aggregator for Channels DVR
- Support for 18+ streaming services
- Smart filtering system
- Web dashboard
- Docker deployment
"

# Push to GitHub
git branch -M main
git push -u origin main
```

### 6. GitHub Settings

**Repository Settings:**
- [ ] Add repository description
- [ ] Add website URL (if you have one)
- [ ] Add topics: `channels-dvr`, `sports`, `streaming`, `epg`, `apple-tv`, `docker`, `python`, `selenium`
- [ ] Disable: Wikis (use docs/ instead)
- [ ] Enable: Issues
- [ ] Enable: Discussions (optional but recommended)

**Branch Protection (optional):**
- [ ] Protect `main` branch
- [ ] Require pull request reviews
- [ ] Require status checks

### 7. Create GitHub Issues Templates

Create `.github/ISSUE_TEMPLATE/bug_report.md`:
```markdown
---
name: Bug report
about: Create a report to help us improve
title: '[BUG] '
labels: bug
assignees: ''
---

**Describe the bug**
A clear description of what the bug is.

**To Reproduce**
Steps to reproduce:
1. Go to '...'
2. Click on '...'
3. See error

**Expected behavior**
What you expected to happen.

**Environment:**
- OS: [e.g., Ubuntu 22.04, Windows 11]
- Docker version: [e.g., 24.0.7]
- FruitDeepLinks version: [e.g., v1.0]
- Streaming device: [e.g., Fire TV 4K]

**Logs:**
```
Paste relevant logs here
```

**Additional context**
Any other context about the problem.
```

Create `.github/ISSUE_TEMPLATE/feature_request.md`:
```markdown
---
name: Feature request
about: Suggest an idea for this project
title: '[FEATURE] '
labels: enhancement
assignees: ''
---

**Is your feature request related to a problem?**
A clear description of the problem. Ex. I'm frustrated when [...]

**Describe the solution you'd like**
A clear description of what you want to happen.

**Describe alternatives you've considered**
Other solutions or features you've considered.

**Additional context**
Any other context, screenshots, or examples.
```

### 8. Create Initial Release

**Tag v1.0:**
```bash
git tag -a v1.0 -m "FruitDeepLinks v1.0 - Initial Release

Features:
- Support for 18+ streaming services
- Smart filtering system (services, sports, leagues)
- Web dashboard on port 6655
- Direct and lane channel modes
- Docker deployment
- Logical service mapping (Apple MLS/MLB, Max, F1TV, etc.)
"

git push origin v1.0
```

**On GitHub:**
- [ ] Go to Releases → Create new release
- [ ] Choose tag: v1.0
- [ ] Title: "FruitDeepLinks v1.0 - Initial Release"
- [ ] Description: (copy from tag message + add highlights)
- [ ] Attach: ZIP of source code (auto-generated)
- [ ] Mark as: Pre-release (since it's beta)

### 9. Update Forum Post

Update your forum post with:
```markdown
**Repository:** https://github.com/kineticman/FruitDeepLinks (Private Beta)
**Status:** Accepting beta testers - DM for access
```

### 10. Optional: GitHub Actions

Create `.github/workflows/docker-build.yml`:
```yaml
name: Docker Build Test

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - name: Build Docker image
      run: docker build -t fruitdeeplinks:test .
    - name: Test container starts
      run: |
        docker run -d --name test fruitdeeplinks:test
        sleep 10
        docker logs test
        docker stop test
```

## Post-Setup Tasks

### Invite Collaborators
- [ ] Invite beta testers as collaborators
- [ ] Set appropriate permissions (Read, Write, or Admin)

### Documentation
- [ ] Create INSTALLATION.md with detailed setup steps
- [ ] Create TROUBLESHOOTING.md with common issues
- [ ] Add screenshots to docs/images/

### Community
- [ ] Enable GitHub Discussions
- [ ] Create discussion categories: Q&A, Ideas, Show & Tell
- [ ] Post welcome message in Discussions

### Monitoring
- [ ] Set up GitHub watch for issues/PRs
- [ ] Consider adding GitHub Star button to README
- [ ] Track metrics (stars, forks, issues)

## Marketing (When Ready to Go Public)

- [ ] Post on Reddit: r/ChannelsDVR, r/cordcutters, r/homelab
- [ ] Post on Channels DVR forums
- [ ] Create demo video/screenshots
- [ ] Tweet announcement
- [ ] Blog post (if you have one)

## Maintenance Schedule

**Weekly:**
- [ ] Review and respond to issues
- [ ] Test with latest Channels DVR updates
- [ ] Check for API changes

**Monthly:**
- [ ] Update dependencies
- [ ] Review and merge PRs
- [ ] Update documentation

**As Needed:**
- [ ] Add new streaming services
- [ ] Fix breaking changes
- [ ] Release new versions

---

**Current Status:**
- Repository: Created ✅
- Files: Ready to upload
- Next: Initial commit and push

**Quick Command Summary:**
```bash
cd C:\projects\FruitDeepLinks
git init
git remote add origin https://github.com/kineticman/FruitDeepLinks.git
git add .
git commit -m "Initial commit: FruitDeepLinks v1.0"
git branch -M main
git push -u origin main
```

Good luck with your launch! 🚀
