@echo off
cd /d C:\inetpub\wwwroot\FlaskSites\Avanzamenti_produzione

.venv\Scripts\python.exe -m app_odp.tasks.generate_kpi_snapshot --month previous >> logs\kpi_snapshot_task.log 2>&1