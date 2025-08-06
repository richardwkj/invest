# Invest Analytics - Automated Stock and Derivatives Analysis Platform

A comprehensive automated analytics platform for analyzing stocks and derivatives, with a phased approach starting from Korean markets (KOSDAQ and KOSPI) and expanding to US markets.

## 🎯 Project Overview

This project aims to create a robust automated analytics system that combines:
- **Data Collection**: Real-time and historical stock data from Korean and US markets
- **AI-Powered Analysis**: Natural language processing for sentiment analysis and automated equity research
- **Technical Indicators**: Custom indicators for investment decision-making
- **Scalable Architecture**: Modular design for easy expansion and maintenance

## 🚀 Development Phases

### Phase 1: Korean Market Database and Data Collection
- **Stock Information**: Collect data from KOSPI and KOSDAQ using `marcap` package
- **Financial Data**: Integrate with DART using `OpenDartReader` for financial statements
- **Data Processing**: Clean and normalize collected data
- **Technical Indicators**: Develop fundamental and technical analysis indicators

### Phase 2: AI Agent Framework for Text Analysis
- **Text Data Collection**: Annual reports, news articles, analyst reports
- **NLP Pipeline**: Sentiment analysis, entity recognition, topic modeling
- **AI Equity Research Analyst**: Automated report generation and investment thesis development
- **Sentiment Analysis**: Real-time news sentiment tracking and analysis

### Phase 3: US Market Expansion
- **US Market Integration**: Yahoo Finance, Alpha Vantage, SEC EDGAR
- **Unified Platform**: Cross-market analysis and correlation studies
- **Advanced Analytics**: Portfolio optimization and risk management systems

## 🛠️ Technology Stack

### Core Technologies
- **Python 3.8+**: Primary programming language
- **SQLAlchemy**: Database ORM and management
- **FastAPI**: Modern web framework for APIs
- **Pandas & NumPy**: Data manipulation and analysis

### Data Sources
- **Korean Markets**: `marcap` package for KOSPI/KOSDAQ data
- **Financial Data**: `OpenDartReader` for DART integration
- **US Markets**: Yahoo Finance API, Alpha Vantage (Phase 3)

### AI/ML Stack
- **Transformers**: Hugging Face for NLP tasks
- **LangChain**: LLM orchestration and AI agents
- **scikit-learn**: Machine learning algorithms
- **spaCy**: Text processing and NLP

### Infrastructure
- **PostgreSQL/SQLite**: Database storage
- **Redis**: Caching and real-time data
- **Docker**: Containerization
- **Prometheus/Grafana**: Monitoring and alerting

## 📁 Project Structure

```
invest/
├── config/                 # Configuration files
├── data/                   # Data storage
│   ├── raw/               # Raw data from APIs
│   ├── processed/         # Cleaned and processed data
│   ├── models/            # Trained ML models
│   └── backups/           # Data backups
├── src/                   # Source code
│   ├── data_collection/   # Data collection modules
│   ├── data_processing/   # Data cleaning and processing
│   ├── analysis/          # Technical and fundamental analysis
│   ├── ai_agents/         # AI-powered analysis agents
│   ├── database/          # Database models and connections
│   ├── api/               # FastAPI application
│   └── utils/             # Utility functions
├── tests/                 # Unit and integration tests
├── notebooks/             # Jupyter notebooks for exploration
├── scripts/               # Utility scripts
├── docs/                  # Documentation
├── docker/                # Containerization
└── monitoring/            # System monitoring
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8 or higher
- Git
- PostgreSQL (optional, SQLite for development)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/invest-analytics.git
   cd invest-analytics
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys and configuration
   ```

5. **Initialize the database**
   ```bash
   python scripts/setup_database.py
   ```

6. **Run the application**
   ```bash
   python -m src.api.main
   ```

## 🔧 Configuration

### Required API Keys
- **DART API Key**: For Korean financial data (required for Phase 1)
- **News API Key**: For sentiment analysis (optional for Phase 2)
- **OpenAI/Anthropic API Key**: For AI analysis (optional for Phase 2)

### Environment Variables
Copy `.env.example` to `.env` and configure:
```bash
# Database
DATABASE_URL=postgresql://username:password@localhost:5432/invest_analytics

# API Keys
DART_API_KEY=your_dart_api_key_here
NEWS_API_KEY=your_news_api_key_here
OPENAI_API_KEY=your_openai_api_key_here

# Application Settings
DEBUG=True
LOG_LEVEL=INFO
```

## 📊 Usage Examples

### Collect Korean Stock Data
```python
from src.data_collection.korean_stocks import KoreanStockCollector

# Initialize collector
collector = KoreanStockCollector()

# Get all KOSPI stocks
kospi_stocks = collector.get_stock_list("KOSPI")

# Get historical data for a specific stock
samsung_data = collector.get_stock_data("005930", "2023-01-01", "2023-12-31")

# Get top 50 stocks by market cap
top_stocks = collector.get_top_stocks("ALL", "market_cap", 50)
```

### Run Technical Analysis
```python
from src.analysis.technical_indicators import TechnicalAnalyzer

# Initialize analyzer
analyzer = TechnicalAnalyzer()

# Calculate technical indicators
indicators = analyzer.calculate_indicators(stock_data)
```

## 🧪 Testing

Run the test suite:
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src

# Run specific test categories
pytest tests/unit/
pytest tests/integration/
```

## 📈 Development Workflow

1. **Data Collection**: Start with Korean market data using marcap
2. **Data Processing**: Clean and normalize collected data
3. **Analysis Development**: Build technical and fundamental indicators
4. **AI Integration**: Add sentiment analysis and AI agents
5. **US Market Expansion**: Extend to US markets
6. **Production Deployment**: Containerize and deploy

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For support and questions:
- Create an issue in the GitHub repository
- Check the documentation in the `docs/` folder
- Review the Jupyter notebooks in `notebooks/` for examples

## 🔮 Roadmap

- [ ] Phase 1: Korean market data collection and processing
- [ ] Phase 2: AI agent framework and sentiment analysis
- [ ] Phase 3: US market integration and unified platform
- [ ] Real-time trading integration
- [ ] Mobile application
- [ ] Advanced ML models for price prediction
- [ ] Global market expansion

---

**Note**: This project is in active development. Please check the current phase and ensure you have the required API keys before starting.
