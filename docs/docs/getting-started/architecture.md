Pontoon consists of several components that work together:

- **Frontend**: [Next.js](https://github.com/vercel/next.js/) web application providing the user interface
- **API**: [FastAPI](https://github.com/fastapi/fastapi) backend handling HTTP requests and business logic
- **Worker**: [Celery](https://github.com/celery/celery) worker processing data transfers asynchronously
- **Beat**: Celery [RedBeat](https://github.com/sibson/redbeat) scheduler managing recurring data transfers
- **Postgres**: Primary database storing configuration and metadata
- **Redis**: Message broker and cache for Celery tasks
