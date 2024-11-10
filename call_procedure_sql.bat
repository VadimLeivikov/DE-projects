@echo off

REM Paths to files and directories:
set SOURCE_FILE="D:\new_daily_data.csv"
set DESTINATION_DIR="C:\Program Files\PostgreSQL\12\data"


REM Copying the report:
echo Copying file %SOURCE_FILE% to %DESTINATION_DIR%...
copy %SOURCE_FILE% %DESTINATION_DIR%

REM Checking if the file was copied successfully:
IF %ERRORLEVEL% NEQ 0 (
    echo Error during the copying 
    echo Press any key for exit...
    pause >nul
    exit /b
) ELSE (
    echo File %SOURCE_FILE% has been copied successfully
)

set PGPASSWORD=****
"C:\Program Files\PostgreSQL\12\bin\psql.exe" -U postgres -d supermarketsales -f "C:\Program Files\PostgreSQL\12\data\call_procedure_script.sql"

REM Checking the execution status:
IF %ERRORLEVEL% NEQ 0 (
    echo We have error during processing SQL procedure
) ELSE (
    echo SQL procedure has been processed successfully
)

REM Exit:
echo Press any key for exit...
pause >nul