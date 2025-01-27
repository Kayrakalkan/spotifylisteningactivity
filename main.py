import os
import threading
import argparse
from dotenv import load_dotenv
from FriendTracker import SpotifyAnalyzer
from FriendVisualizer import SpotifyVisualizer
import logging
import time
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
from jinja2 import Template

# Load environment variables
load_dotenv()

def setup_logging():
    """Configure logging for the application"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('spotify_activity.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def run_data_collection(analyzer):
    """Run the data collection loop in a separate thread"""
    try:
        analyzer.run_collection_loop()
    except Exception as e:
        logging.error(f"Data collection error: {e}")

def run_dashboard_server(port=8000):
    """Run a simple HTTP server for the dashboard"""
    server = HTTPServer(('localhost', port), SimpleHTTPRequestHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    return server

def create_user_dashboard(analyzer, visualizer, user_uri: str) -> None:
    """Generate a user-specific dashboard"""
    template = Template("""
    <html>
        <head>
            <title>User Activity Dashboard</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                /* ... existing styles ... */
                .user-search {
                    margin: 20px 0;
                    text-align: center;
                }
                .user-search input {
                    padding: 8px;
                    width: 300px;
                }
            </style>
        </head>
        <body>
            <h1>User Activity Dashboard</h1>
            <div class="user-search">
                <input type="text" id="userSearch" placeholder="Enter Spotify User URI">
                <button onclick="loadUserData()">Search</button>
            </div>
            <div class="chart-container">
                <div id="userHeatmap"></div>
            </div>
            <div class="chart-container">
                <div id="userTrends"></div>
            </div>
            <script>
                var userHeatmap = {{ heatmap | safe }};
                var userTrends = {{ trends | safe }};
                
                Plotly.newPlot('userHeatmap', userHeatmap.data, userHeatmap.layout);
                Plotly.newPlot('userTrends', userTrends.data, userTrends.layout);
                
                function loadUserData() {
                    var userUri = document.getElementById('userSearch').value;
                    window.location.href = '/dashboard.html?user=' + encodeURIComponent(userUri);
                }
            </script>
        </body>
    </html>
    """)
    
    # Generate visualizations
    heatmap = visualizer.create_user_activity_heatmap(user_uri)
    trends = visualizer.create_user_dashboard(user_uri)
    
    # Save to file
    with open('user_dashboard.html', 'w') as f:
        f.write(template.render(
            heatmap=heatmap.to_json(),
            trends=trends.to_json()
        ))

def cleanup_connections(analyzer):
    """Clean up database connections"""
    try:
        analyzer.close_db_connection()
    except Exception as e:
        print(f"Error cleaning up connections: {e}")
        # logger.error(f"Error cleaning up connections: {e}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Spotify Friend Activity Analyzer')
    parser.add_argument('--port', type=int, default=8000, help='Dashboard server port')
    parser.add_argument('--db', type=str, default='spotify_activity.db', help='Database file path')
    parser.add_argument('--interval', type=int, default=60, help='Data collection interval in seconds')
    args = parser.parse_args()

    # Setup logging
    logger = setup_logging()
    logger.info("Starting Spotify Friend Activity Analyzer")

    # Get configuration
    bearer_token = os.getenv('SPOTIFY_BEARER_TOKEN')
    if not bearer_token:
        logger.error("SPOTIFY_BEARER_TOKEN not found in environment variables")
        return
    
    # Add this to main.py after loading the token
    logger.info(f"Bearer token loaded: {bearer_token[:10]}...")  # Shows first 10 chars

    try:
        # Initialize analyzer and visualizer
        analyzer = SpotifyAnalyzer(
            database_path=args.db,
            bearer_token=bearer_token
        )
        visualizer = SpotifyVisualizer(analyzer)

        # Start data collection in a separate thread
        collection_thread = threading.Thread(
            target=run_data_collection,
            args=(analyzer,),
            daemon=True
        )
        collection_thread.start()
        logger.info("Data collection thread started")

        # Create initial dashboard
        logger.info("Generating initial dashboard...")
        visualizer.generate_html_dashboard()

        # Start dashboard update loop in a separate thread
        def update_dashboard():
            while True:
                try:
                    visualizer.generate_html_dashboard()
                    time.sleep(args.interval)
                except Exception as e:
                    logger.error(f"Dashboard update error: {e}")
                    time.sleep(args.interval)

        dashboard_thread = threading.Thread(
            target=update_dashboard,
            daemon=True
        )
        dashboard_thread.start()
        logger.info("Dashboard update thread started")

        # Start HTTP server for dashboard
        server = run_dashboard_server(args.port)
        dashboard_url = f"http://localhost:{args.port}/dashboard.html"
        logger.info(f"Dashboard available at: {dashboard_url}")

        # Open dashboard in default browser
        webbrowser.open(dashboard_url)

        # Keep the main thread alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
        cleanup_connections(analyzer)
    except Exception as e:
        logger.error(f"Application error: {e}")
        cleanup_connections(analyzer)
    finally:
        cleanup_connections(analyzer)

if __name__ == "__main__":
    main()