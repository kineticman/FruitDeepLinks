# Contributing to FruitDeepLinks

Thank you for your interest in contributing! This is currently a private repository, but we welcome contributions from invited collaborators.

## Getting Started

### Development Environment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/kineticman/FruitDeepLinks.git
   cd FruitDeepLinks
   ```

2. **Set up environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

3. **Run with Docker (recommended):**
   ```bash
   docker-compose up -d
   docker logs fruitdeeplinks -f
   ```

4. **Or run locally:**
   ```bash
   pip install -r requirements.txt
   python bin/daily_refresh.py
   ```

## Code Organization

```
bin/                    # Python scripts
├── daily_refresh.py    # Main orchestrator
├── appletv_to_peacock.py         # Apple TV scraper
├── peacock_export_hybrid.py      # Direct channels
├── peacock_export_lanes.py       # Lane channels
├── fruitdeeplinks_server.py      # Web dashboard
├── filter_integration.py         # Filtering logic
├── logical_service_mapper.py     # Service mapping
└── provider_utils.py             # Provider helpers
```

## Development Guidelines

### Code Style

- **Python:** PEP 8 style guide
- **Max line length:** 100 characters
- **Docstrings:** Google style
- **Type hints:** Encouraged but not required

### Testing

```bash
# Run manual tests
docker exec fruitdeeplinks python3 /app/bin/daily_refresh.py

# Check database
docker exec fruitdeeplinks sqlite3 /app/data/fruit_events.db "SELECT COUNT(*) FROM events"

# Verify exports
docker exec fruitdeeplinks ls -la /app/out/
```

### Making Changes

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**

3. **Test thoroughly:**
   - Run full refresh
   - Check exports
   - Verify web dashboard
   - Test filtering

4. **Commit with clear messages:**
   ```bash
   git commit -m "Add: Description of what you added"
   git commit -m "Fix: Description of what you fixed"
   git commit -m "Update: Description of what you updated"
   ```

5. **Push and create pull request:**
   ```bash
   git push origin feature/your-feature-name
   ```

## Areas for Contribution

### High Priority
- [ ] Additional service support (YouTube TV, FuboTV, etc.)
- [ ] Team-based filtering
- [ ] Chrome Capture / AH4C integration
- [ ] Better error handling and logging

### Medium Priority
- [ ] Multi-user profiles
- [ ] Mobile-friendly web dashboard
- [ ] API documentation
- [ ] Unit tests

### Documentation
- [ ] Installation guides for different platforms
- [ ] Troubleshooting guides
- [ ] Video tutorials
- [ ] API documentation

## Adding New Services

To add support for a new streaming service:

1. **Identify the deeplink scheme:**
   - Research the service's URL scheme (e.g., `newservice://`)
   - Test on target platform (Fire TV, Apple TV, etc.)

2. **Update provider_utils.py:**
   ```python
   DEFAULT_PROVIDER_PRIORITY = [
       'sportsonespn',
       # ... existing services ...
       'newservice',  # Add here
   ]
   
   SERVICE_DISPLAY_NAMES = {
       # ... existing services ...
       'newservice': 'New Service Name',
   }
   ```

3. **Update logical_service_mapper.py** (if web-based):
   ```python
   LOGICAL_SERVICE_MAP = {
       # ... existing mappings ...
       'newservice.com': 'newservice',
   }
   ```

4. **Test thoroughly:**
   - Verify deeplinks work on target platform
   - Check filtering system includes new service
   - Update documentation

5. **Update SERVICE_CATALOG.md** with service details

## Debugging Tips

### Enable Debug Logging

Edit `docker-compose.yml`:
```yaml
environment:
  - LOG_LEVEL=DEBUG
```

### Database Inspection

```bash
# View events
docker exec fruitdeeplinks sqlite3 /app/data/fruit_events.db \
  "SELECT * FROM events WHERE title LIKE '%Lakers%'"

# View playables
docker exec fruitdeeplinks sqlite3 /app/data/fruit_events.db \
  "SELECT * FROM playables WHERE provider='newservice'"

# View user preferences
docker exec fruitdeeplinks sqlite3 /app/data/fruit_events.db \
  "SELECT * FROM user_preferences"
```

### Web Dashboard

Access logs at: `http://your-server:6655/logs`

### Manual Script Execution

```bash
# Run specific scripts
docker exec fruitdeeplinks python3 /app/bin/appletv_to_peacock.py
docker exec fruitdeeplinks python3 /app/bin/peacock_export_hybrid.py
docker exec fruitdeeplinks python3 /app/bin/logical_service_mapper.py
```

## Questions?

- **Issues:** Use GitHub Issues for bugs
- **Discussions:** Use GitHub Discussions for ideas
- **Direct contact:** Reach out to repository maintainers

## Code of Conduct

- Be respectful and constructive
- Focus on the code, not the person
- Welcome newcomers and help them learn
- Keep discussions on-topic

Thank you for contributing! 🙏
