$InformationPreference = "Continue"
$ErrorActionPreference = "Stop"

$Config = Get-Content .\config.json | ConvertFrom-Json

# Determine whether an Entity is available for synchronization
# Note: based on visibility in Dataverse portal
function IsValidEntity {
    param (
        $Entity
    )

    $Entity.IsIntersect -eq $False `
        -and $Entity.IsLogicalEntity -eq $False `
        -and $Entity.PrimaryNameAttribute `
        -and $Entity.PrimaryNameAttribute -ne '' `
        -and $Entity.ObjectTypeCode -gt 0 `
        -and $Entity.ObjectTypeCode -ne 4712 `
        -and $Entity.ObjectTypeCode -ne 4724 `
        -and $Entity.ObjectTypeCode -ne 9933 `
        -and $Entity.ObjectTypeCode -ne 9934 `
        -and $Entity.ObjectTypeCode -ne 9935 `
        -and $Entity.ObjectTypeCode -ne 9947 `
        -and $Entity.ObjectTypeCode -ne 9945 `
        -and $Entity.ObjectTypeCode -ne 9944 `
        -and $Entity.ObjectTypeCode -ne 9942 `
        -and $Entity.ObjectTypeCode -ne 9951 `
        -and $Entity.ObjectTypeCode -ne 2016 `
        -and $Entity.ObjectTypeCode -ne 9949 `
        -and $Entity.ObjectTypeCode -ne 9866 `
        -and $Entity.ObjectTypeCode -ne 9867 `
        -and $Entity.ObjectTypeCode -ne 9868 `
        -and ( `
            $Entity.IsCustomizable.Value -eq $True `
            -or $Entity.IsCustomEntity -eq $True `
            -or $Entity.IsManaged -eq $False `
            -or $Entity.IsMappable.Value -eq $True `
            -or $Entity.IsRenameable.Value -eq $True `
    ) `

}

# CRM Schema information output folder (create if missing)
if (-not (Test-Path -Path $($Config.CrmSchemaPath))) {
    mkdir $($Config.CrmSchemaPath) | Out-Null
}


# Check CRM connection
if (-not $global:CrmConnection) {
    .\Login-Crm.ps1
}

$ServiceUri = $global:CrmConnection.ConnectedOrgPublishedEndpoints['OrganizationDataService']

# Request headers
$Headers = @{ `
        Authorization      = "Bearer $($global:CrmConnection.CurrentAccessToken)"; `
        Accept             = "application/json"; `
        "OData-Version"    = "4.0"; `
        "OData-MaxVersion" = "4.0" `

}

# Import tables to ingest
$TablesToIngest = New-Object System.Collections.Generic.HashSet[string]

$Config.TablesToIngest | ForEach-Object {
    $TablesToIngest.Add($_.Name) | Out-Null
}

# Set of referenced tables
$ReferencedTables = New-Object System.Collections.Generic.HashSet[string]

# Find all tables referenced by the tables to be ingested.
# Note: Referenced tables are needed to populate lookup columns
$TablesToIngest | ForEach-Object {
    $TableName = $_

    Write-Information "Retrieving relationship of table $TableName"

    $Response = Invoke-WebRequest `
        "$($ServiceUri)EntityDefinitions(LogicalName='$TableName')?`$expand=ManyToOneRelationships,Attributes" `
        -Headers $Headers

    $Entity = ($Response.content | ConvertFrom-Json)

    if ( IsValidEntity $Entity ) {
        $Entity.ManyToOneRelationships | ForEach-Object {
            if ($_.ReferencedEntity -ne "owner") {
                $ReferencedTables.Add($_.ReferencedEntity) | Out-Null
            }
            else {
                # The "owner" entity is a reference to systemuser and team entities in a polymorphic lookup column
                $ReferencedTables.Add("systemuser") | Out-Null
                $ReferencedTables.Add("team") | Out-Null
            }
        }
    }
    else {
        Write-Information "Skipping $($Entity.LogicalName)"
    }
}

# Add referenced tables to the list of the tables to ingest
$ReferencedTables | ForEach-Object {
    $TablesToIngest.Add($_) | Out-Null
}

# Download metadata for each table to ingest, including referenced tables
# Save the list of tables to the path set in TablesToSyncOutputPath (config.json),
# including whether change tracking is enabled
$TablesToIngest | Sort-Object | ForEach-Object {
    $TableName = $_

    Write-Information "Processing $TableName"

    $Response = Invoke-WebRequest `
        "$($ServiceUri)EntityDefinitions(LogicalName='$TableName')?`$expand=ManyToOneRelationships,Attributes" `
        -Headers $Headers

    $Entity = ($Response.content | ConvertFrom-Json)

    if ( IsValidEntity $Entity ) {
        $Entity | ConvertTo-Json | Out-File "$($Config.CrmSchemaPath)\Entity_$TableName.json" -Encoding utf8

        [PSCustomObject]@{
            LogicalName           = $Entity.LogicalName;
            ChangeTrackingEnabled = $Entity.ChangeTrackingEnabled
        }    
    }
    else {
        Write-Information "Skipping $($Entity.LogicalName)"
    }
} | Export-Csv `
    -Path $Config.TablesToSyncOutputPath `
    -NoTypeInformation `
    -Encoding utf8