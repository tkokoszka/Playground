# PowerShell 7 script to runs checks on the code.
# Execute before doing commit or creating Pull Request, from the project directory, e.g.
# $ script\presubmit.ps1

# Check if running from proper directory location.
if (-not (Test-Path -Path 'scripts' -PathType Container)) {
    Write-Error "This script must be run from the project root directory."
    exit 1
}

function RunOrDie {
    param (
        [Parameter(ValueFromRemainingArguments=$true)]
        [string[]]$Cmd
    )
    Write-Output "================================================================================================"
    Write-Output "== $Cmd"
    Invoke-Expression $($Cmd -join " ")
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Command '$Cmd' failed"
        exit 1
    }
}

# PowerShell insterprets arguments starting with - (dash) as a switch and does not pass it to the catch it all
# param (ValueFromRemainingArguments). The only way around I found is to wrap those in quotes.
RunOrDie pylint "-v" **/*.py

Write-Output "All checks completed."