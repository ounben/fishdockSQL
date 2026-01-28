fishdockSQL
https://youtu.be/MhAD1MlX8pc

[<img src="https://img.youtube.com/vi/MhAD1MlX8pc/hqdefault.jpg" width="600" height="300"
/>](https://www.youtube.com/embed/MhAD1MlX8pc)

Real-time monitoring for Lichess Fishnet workers. Tracks Docker containers, hardware telemetry, and global chess activity.

Features
NPS to SI: Automatically converts knps/core to Nodes per second (nps).

Hardware Telemetry: Logs PPT (Watt), EDC (Ampere), and Temperature (Celsius) via LibreHardwareMonitor JSON API.

Global Populations: Scrapes live user counts from Lichess and Chess.com (via JS variable extraction).

Reset Protection: Detects container crashes/resets and restores cumulative node statistics automatically.

Setup
Requirements: pip install docker requests psycopg2-binary

Database: PostgreSQL (Tables metrics and worker_stats are created on startup).

Config: Set DB credentials (Host, Name, User, Password) in the script header.

Logic
Main Loop: Scans Docker socket for fishnet- containers every 5s.

Threading: Separate thread per container log-stream; one background worker for web stats.

Output: SQL insert per batch + SI-compliant console logs.

