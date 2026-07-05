@echo off
:: Runs the Tandem sample application (requires Docker).
::
:: Usage (from any directory):
::   tandem-sample\run.cmd
::   run.cmd           (when inside tandem-sample\)
::
:: The script navigates to the project root automatically so that Gradle and
:: Testcontainers can locate the schema files regardless of where you call it from.

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."

call gradlew.bat :tandem-sample:run --console=plain
