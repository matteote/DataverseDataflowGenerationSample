try {
    $global:CrmConnection = Get-CrmConnection -InteractiveMode
}
catch [System.Management.Automation.CommandNotFoundException] {
    Write-Error "Get-CrmConnection could not be found, run Install-Requirements.ps1"
}