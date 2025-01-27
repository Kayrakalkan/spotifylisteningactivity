import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from typing import Dict, List
from jinja2 import Template

class SpotifyVisualizer:
    def __init__(self, analyzer):
        self.analyzer = analyzer
        
    def create_activity_heatmap(self) -> go.Figure:
        """Create an activity heatmap showing listening patterns by hour"""
        df = self.analyzer.get_hourly_activity_heatmap()
        
        # Create an empty figure if no data
        if df.empty:
            fig = go.Figure()
            fig.update_layout(
                title='No Listening Activity Data Available',
                annotations=[{
                    'text': 'Waiting for data...',
                    'xref': 'paper',
                    'yref': 'paper',
                    'showarrow': False,
                    'font': {'size': 20}
                }]
            )
            return fig
        
        # Pivot the data for the heatmap
        heatmap_data = df.pivot(
            index='user_name',
            columns='hour',
            values='activity_count'
        ).fillna(0)

        fig = go.Figure(data=go.Heatmap(
            z=heatmap_data.values,
            x=heatmap_data.columns,
            y=heatmap_data.index,
            colorscale='Viridis'
        ))

        fig.update_layout(
            title='Hourly Listening Activity by User',
            xaxis_title='Hour of Day',
            yaxis_title='User',
            height=400
        )
        return fig

    def create_recent_trends_dashboard(self, minutes: int = 10) -> go.Figure:
        """Create a dashboard of recent listening trends"""
        df = self.analyzer.analyze_recent_activity(minutes)
        
        # Create empty dashboard if no data
        if df.empty:
            fig = go.Figure()
            fig.update_layout(
                title='No Recent Activity Data Available',
                annotations=[{
                    'text': 'Waiting for data...',
                    'xref': 'paper',
                    'yref': 'paper',
                    'showarrow': False,
                    'font': {'size': 20}
                }]
            )
            return fig
        
        # Create subplots with specific types
        fig = make_subplots(
            rows=2, cols=2,
            specs=[
                [{"type": "xy"}, {"type": "xy"}],
                [{"type": "domain"}, {"type": "domain"}]
            ],
            subplot_titles=(
                'Top Artists', 'Top Tracks',
                'User Activity', 'Context Distribution'
            )
        )

        # Top Artists
        artist_counts = df['artist_name'].value_counts().head(5)
        if not artist_counts.empty:
            fig.add_trace(
                go.Bar(x=artist_counts.values, y=artist_counts.index, orientation='h'),
                row=1, col=1
            )

        # Top Tracks
        track_counts = df['track_name'].value_counts().head(5)
        if not track_counts.empty:
            fig.add_trace(
                go.Bar(x=track_counts.values, y=track_counts.index, orientation='h'),
                row=1, col=2
            )

        # User Activity
        user_counts = df['user_name'].value_counts()
        if not user_counts.empty:
            fig.add_trace(
                go.Pie(labels=user_counts.index, values=user_counts.values),
                row=2, col=1
            )

        # Context Distribution
        context_counts = df['context_name'].value_counts()
        if not context_counts.empty:
            fig.add_trace(
                go.Pie(labels=context_counts.index, values=context_counts.values),
                row=2, col=2
            )

        fig.update_layout(
            height=800,
            showlegend=True,
            title_text=f"Last {minutes} Minutes Listening Trends"
        )

        # Update layout for better spacing
        fig.update_layout(
            margin=dict(t=100, b=50, l=50, r=50),
            grid={'rows': 2, 'columns': 2, 'pattern': 'independent'}
        )

        return fig

    def create_all_time_activity_graph(self) -> go.Figure:
        """Create a comprehensive graph showing all-time listening activity"""
        df = self.analyzer.get_all_time_activity()
        
        # Handle empty data
        if df.empty:
            fig = go.Figure()
            fig.update_layout(
                title='No All-Time Activity Data Available',
                annotations=[{
                    'text': 'Waiting for data...',
                    'xref': 'paper',
                    'yref': 'paper',
                    'showarrow': False,
                    'font': {'size': 20}
                }]
            )
            return fig
        
        # Convert timestamps to datetime
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Create subplots
        fig = make_subplots(
            rows=3, cols=2,
            specs=[
                [{"type": "xy"}, {"type": "xy"}],
                [{"type": "xy"}, {"type": "xy"}],
                [{"colspan": 2, "type": "domain"}, None],
            ],
            subplot_titles=(
                'Daily Listening Activity', 'Weekly Pattern',
                'Top Artists', 'Top Tracks',
                'Activity Timeline'
            ),
            vertical_spacing=0.12
        )
        
        # 1. Daily Activity Timeline
        daily_activity = df.groupby('date')['track_name'].count().reset_index()
        daily_activity['date'] = pd.to_datetime(daily_activity['date'])
        fig.add_trace(
            go.Scatter(
                x=daily_activity['date'],
                y=daily_activity['track_name'],
                mode='lines',
                name='Daily Tracks',
                line=dict(color='#1DB954')
            ),
            row=1, col=1
        )
        
        # 2. Weekly Pattern
        days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        hourly_pattern = df.pivot_table(
            index='day_of_week',
            columns='hour',
            values='track_name',
            aggfunc='count'
        ).fillna(0)
        hourly_pattern.index = [days[int(i)] for i in hourly_pattern.index]
        
        fig.add_trace(
            go.Heatmap(
                z=hourly_pattern.values,
                x=hourly_pattern.columns,
                y=hourly_pattern.index,
                colorscale='Viridis',
                name='Weekly Pattern'
            ),
            row=1, col=2
        )
        
        # 3. Top Artists
        artist_counts = df['artist_name'].value_counts().head(10)
        fig.add_trace(
            go.Bar(
                x=artist_counts.values,
                y=artist_counts.index,
                orientation='h',
                name='Top Artists',
                marker_color='#1DB954'
            ),
            row=2, col=1
        )
        
        # 4. Top Tracks
        track_counts = df['track_name'].value_counts().head(10)
        fig.add_trace(
            go.Bar(
                x=track_counts.values,
                y=track_counts.index,
                orientation='h',
                name='Top Tracks',
                marker_color='#1DB954'
            ),
            row=2, col=2
        )
        
        # 5. User Activity Distribution
        user_activity = df.groupby('user_name')['track_name'].count()
        fig.add_trace(
            go.Pie(
                labels=user_activity.index,
                values=user_activity.values,
                name='User Activity',
                hole=0.4
            ),
            row=3, col=1
        )
        
        # Update layout
        fig.update_layout(
            height=1200,
            showlegend=True,
            title_text="All-Time Listening Activity Analysis",
            title_x=0.5,
            template='plotly_white'
        )
        
        # Update axes labels
        fig.update_xaxes(title_text="Date", row=1, col=1)
        fig.update_yaxes(title_text="Tracks Played", row=1, col=1)
        fig.update_xaxes(title_text="Hour of Day", row=1, col=2)
        fig.update_yaxes(title_text="Day of Week", row=1, col=2)
        fig.update_xaxes(title_text="Tracks Played", row=2, col=1)
        fig.update_xaxes(title_text="Tracks Played", row=2, col=2)
        
        return fig

    def generate_html_dashboard(self, output_path: str = "dashboard.html"):
        """Generate a complete HTML dashboard with all visualizations"""
        heatmap = self.create_activity_heatmap()
        trends = self.create_recent_trends_dashboard()
        all_time = self.create_all_time_activity_graph()
        
        template = Template("""
        <html>
            <head>
                <title>Spotify Friend Activity Dashboard</title>
                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                <style>
                    body { max-width: 1400px; margin: 0 auto; padding: 20px; font-family: Arial, sans-serif; }
                    h1 { text-align: center; color: #1DB954; }
                    .chart-container { margin: 20px 0; border: 1px solid #ddd; padding: 15px; border-radius: 8px; }
                    .section-title { color: #1DB954; margin: 30px 0 15px; text-align: center; }
                </style>
            </head>
            <body>
                <h1>Spotify Friend Activity Dashboard</h1>
                
                <h2 class="section-title">Recent Activity</h2>
                <div class="chart-container">
                    <div id="heatmap"></div>
                </div>
                <div class="chart-container">
                    <div id="trends"></div>
                </div>
                
                <h2 class="section-title">Historical Analysis</h2>
                <div class="chart-container">
                    <div id="allTimeActivity"></div>
                </div>
                
                <script>
                    var heatmap = {{ heatmap | safe }};
                    var trends = {{ trends | safe }};
                    var allTimeActivity = {{ all_time | safe }};
                    
                    Plotly.newPlot('heatmap', heatmap.data, heatmap.layout);
                    Plotly.newPlot('trends', trends.data, trends.layout);
                    Plotly.newPlot('allTimeActivity', allTimeActivity.data, allTimeActivity.layout);
                </script>
            </body>
        </html>
        """)

        # Convert Plotly figures to JSON
        heatmap_json = heatmap.to_json()
        trends_json = trends.to_json()
        all_time_json = all_time.to_json()

        with open(output_path, 'w') as f:
            f.write(template.render(
                heatmap=heatmap_json,
                trends=trends_json,
                all_time=all_time_json
            ))

    def create_user_activity_heatmap(self, user_uri: str) -> go.Figure:
        """Create an activity heatmap for a specific user"""
        df = self.analyzer.get_user_activity(user_uri)
        
        # Handle empty data
        if df.empty:
            fig = go.Figure()
            fig.update_layout(
                title='No Activity Data Available for User',
                annotations=[{
                    'text': 'Waiting for data...',
                    'xref': 'paper',
                    'yref': 'paper',
                    'showarrow': False,
                    'font': {'size': 20}
                }]
            )
            return fig
        
        # Convert timestamps to hours
        df['hour'] = pd.to_datetime(df['timestamp'], unit='ms').dt.hour
        hourly_counts = df.groupby('hour').size().reset_index(name='count')
        
        fig = go.Figure(data=go.Bar(
            x=hourly_counts['hour'],
            y=hourly_counts['count'],
            name='Listening Activity'
        ))

        fig.update_layout(
            title=f'Hourly Listening Activity',
            xaxis_title='Hour of Day',
            yaxis_title='Number of Tracks',
            height=400
        )
        return fig

    def create_user_dashboard(self, user_uri: str) -> go.Figure:
        """Create a dashboard for a specific user"""
        df = self.analyzer.get_user_activity(user_uri)
        
        # Handle empty data
        if df.empty:
            fig = go.Figure()
            fig.update_layout(
                title='No Activity Data Available for User',
                annotations=[{
                    'text': 'Waiting for data...',
                    'xref': 'paper',
                    'yref': 'paper',
                    'showarrow': False,
                    'font': {'size': 20}
                }]
            )
            return fig
        
        fig = make_subplots(
            rows=2, cols=2,
            specs=[
                [{"type": "xy"}, {"type": "xy"}],
                [{"type": "xy"}, {"type": "domain"}]
            ],
            subplot_titles=(
                'Top Artists', 'Top Tracks',
                'Activity Timeline', 'Context Distribution'
            )
        )
        
        # Top Artists
        artist_counts = df['artist_name'].value_counts().head(5)
        if not artist_counts.empty:
            fig.add_trace(
                go.Bar(x=artist_counts.values, y=artist_counts.index, orientation='h'),
                row=1, col=1
            )

        # Top Tracks
        track_counts = df['track_name'].value_counts().head(5)
        if not track_counts.empty:
            fig.add_trace(
                go.Bar(x=track_counts.values, y=track_counts.index, orientation='h'),
                row=1, col=2
            )

        # Activity Timeline
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        timeline = df.set_index('datetime')['track_name'].resample('1H').count()
        fig.add_trace(
            go.Scatter(x=timeline.index, y=timeline.values, mode='lines'),
            row=2, col=1
        )

        # Context Distribution
        context_counts = df['context_name'].value_counts()
        if not context_counts.empty:
            fig.add_trace(
                go.Pie(labels=context_counts.index, values=context_counts.values),
                row=2, col=2
            )

        fig.update_layout(
            height=800,
            showlegend=True,
            title_text=f"User Listening Activity"
        )

        return fig