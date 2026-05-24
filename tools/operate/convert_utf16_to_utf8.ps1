# Convert all .sql files in a folder (and its subfolders) from UTF-16
# to UTF-8, in place. Intended for the local workstation BEFORE
# uploading corpus files to OneLake -- the in-Fabric conversion path
# (notebookutils.fs.put) proved unreliable on the lakehouse mount.
#
# Usage from PowerShell:
#   cd "C:\path\to\local\export\folder"
#   .\convert_utf16_to_utf8.ps1
#
# Or supply a path:
#   .\convert_utf16_to_utf8.ps1 -Path "C:\path\to\export\folder"
#
# After conversion, upload the folder to /lakehouse/.../data/mychart_views/
# (overwriting the original UTF-16 versions), then verify with
# tools/operate/check_corpus_encoding.py in the Fabric notebook -- it
# should report "clean UTF-8 / ASCII" and your parser failures will
# resolve.
#
# Long-term, prefer `mssql-scripter` which defaults to UTF-8 and
# bypasses this trap entirely:
#   pip install mssql-scripter
#   mssql-scripter -S <server> -d <database> --integrated-auth `
#       --include-objects dbo.vw_mychart% dbo.sp_mychart% `
#       --file-per-object --file-path C:\path\to\export\folder

param(
    [string]$Path = "."
)

$converted = 0
$skipped = 0

Get-ChildItem -Path $Path -Filter "*.sql" -Recurse | ForEach-Object {
    # Read with UTF-16 (PowerShell's "Unicode" encoding label).
    $content = Get-Content -LiteralPath $_.FullName -Encoding Unicode -Raw

    # Write back as UTF-8 WITHOUT BOM.
    # PowerShell 5.x: "UTF8" encoding writes WITH BOM by default;
    # PowerShell 7+: "UTF8" writes without BOM. To be safe across
    # versions we explicitly write a BOM-less UTF-8 byte stream.
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($_.FullName, $content, $utf8NoBom)

    Write-Host "Converted: $($_.Name)"
    $converted++
}

Write-Host ""
Write-Host "Done. Converted $converted files."
Write-Host "Now re-upload the folder to /lakehouse/.../data/mychart_views/"
Write-Host "and run check_corpus_encoding in the notebook to verify."
