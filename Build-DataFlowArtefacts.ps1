$InformationPreference = "Continue"
$ErrorActionPreference = "Stop"

$Config = Get-Content .\config.json | ConvertFrom-Json

function Get-DataLakeFile {
    param (
        $Path
    )

    $FullPath = "$($Config.DataLakeCachePath)\$Path"

    if (-not $Global:DataLakeStorageContext) {
        $Global:DataLakeStorageContext = New-AzStorageContext `
            -StorageAccountName $Config.StorageAccount `
            -UseConnectedAccount
    }

    if (-not (Test-Path -Path $FullPath)) {
        $Folder = Split-Path $FullPath
        if (-not (Test-Path -Path $Folder)) {
            mkdir $Folder | out-null
        }

        Get-AzDataLakeGen2ItemContent `
            -Context $Global:DataLakeStorageContext `
            -FileSystem $Config.FileSystem `
            -path $Path.Replace("\", "/") `
            -Destination $FullPath | Out-Null
    }

    Get-Content $FullPath
}

function Get-Table {
    param (
        $TableName,
        [Switch]
        $ExpandReferences = $false
    )

    Write-Information "Retrieving metadata for table $TableName"

    $Table = @{
        Name = $TableName
    }

    $DataLakeModel = Get-DataLakeFile "Microsoft.Athena.TrickleFeedService\$TableName-model.json" `
    | ConvertFrom-Json

    $DataLakeMetadata = Get-DataLakeFile "Microsoft.Athena.TrickleFeedService\$TableName-EntityMetadata.json" `
    | ConvertFrom-Json
    $DataLakeMetadataAttributes = @{}
    foreach ($Attribute in $DataLakeMetadata.AttributeMetadata) {
        $DataLakeMetadataAttributes[$Attribute.AttributeName] = $Attribute
    }

    $OptionSets = [System.Collections.Generic.HashSet[string]] (
        $DataLakeMetadata.OptionSetMetadata, $DataLakeMetadata.GlobalOptionSetMetadata `
        | ForEach-Object { $_ | ForEach-Object { $_.OptionSetName } }`
        | Get-Unique
    )

    $SourceMetadata = (Get-Content ".\$($Config.CrmSchemaPath)\Entity_$TableName.json" | ConvertFrom-Json)
    $SourceMetadataAttributes = @{}
    foreach ($Attribute in $SourceMetadata.Attributes) {
        $SourceMetadataAttributes[$Attribute.LogicalName] = $Attribute
    }

    $ColumnReferences = @{}
    $Table.ReferencedEntities = New-Object System.Collections.Generic.HashSet[string]

    if ($ExpandReferences) {
        foreach ($Reference in $DataLakeMetadata.TargetMetadata) {
            $ReferencedTableMetadataFile = ".\$($Config.CrmSchemaPath)\Entity_$($Reference.ReferencedEntity).json"
            if (Test-Path -Path $ReferencedTableMetadataFile) {
                if ($Reference.ReferencedEntity -notin $Config.TablesToIgnore) {
                    $ReferencedTableMetadata = (Get-Content $ReferencedTableMetadataFile | ConvertFrom-Json)
    
                    $ReferencedAttribute = $Reference.ReferencedAttribute
    
                    if (-not $ReferencedAttribute) {
                        $ReferencedAttribute = $ReferencedTableMetadata.PrimaryIdAttribute
                    }
    
                    $ColumnReferences[$Reference.AttributeName] += @(
                        [PSCustomObject]@{
                            ReferencedEntity              = $Reference.ReferencedEntity;
                            ReferencedAttribute           = $ReferencedAttribute;
                            ReferencedEntityNameAttribute = $ReferencedTableMetadata.PrimaryNameAttribute
                        }
                    )
    
                    $Table.ReferencedEntities.Add($Reference.ReferencedEntity) | Out-Null
                }    
            }
        }
    }

    $Table.HasOptionSets = $false

    $Table.Columns = foreach ($DataLakeModelColumn in $DataLakeModel.entities[0].attributes) {
        $ColumnName = $DataLakeModelColumn.name
        $IsMultiSelectPicklist = $false
        $TruncateColumn = $false
        switch ($DataLakeModelColumn.dataType) {
            "boolean" {
                $DataFlowDataType = "boolean"
                $SqlDataType = "bit"
                break
            }
            "dateTime" {
                $DataFlowDataType = "timestamp"
                $SqlDataType = "datetime"
                break
            }
            "decimal" {
                switch ($DataLakeMetadataAttributes[$ColumnName].AttributeType) {
                    "Double" {
                        $DataFlowDataType = "double"
                        $SqlDataType = "float"
                        break
                    }
                    "Money" {
                        $DataFlowDataType = "decimal(38,4)"
                        $SqlDataType = "money"
                        break
                    }
                    "Decimal" {
                        $Precision = $SourceMetadataAttributes[$ColumnName].Precision
                        $DataFlowDataType = "decimal(38,$Precision)"
                        $SqlDataType = "decimal(38,$Precision)"
                        break
                    }
                    Default {
                        Write-Warning "Table $TableName - Column $ColumnName - Unknown Decimal datatype $($DataLakeMetadataAttributes[$ColumnName].AttributeType)"
                        $Precision = $SourceMetadataAttributes[$ColumnName].Precision
                        $DataFlowDataType = "decimal(38,$Precision)"
                        $SqlDataType = "decimal(38,$Precision)"
                    }
                }
            }
            "guid" {
                $DataFlowDataType = "string"
                $SqlDataType = "nvarchar(4000)"
                break
            }
            "int64" {
                $DataFlowDataType = "long"
                $SqlDataType = "bigint"
                break
            }
            "string" {
                switch ($DataLakeMetadataAttributes[$ColumnName].AttributeType) {
                    "EntityName" {
                        $DataFlowDataType = "string"
                        $SqlDataType = "nvarchar(4000)"
                        break
                    }
                    "Memo" {
                        $DataFlowDataType = "string"
                        $SqlDataType = "nvarchar(4000)"
                        $TruncateColumn = $true
                        break
                    }
                    "String" {
                        $DataFlowDataType = "string"
                        $SqlDataType = "nvarchar(4000)"
                        break
                    }
                    "Virtual" {
                        $DataFlowDataType = "string"
                        $SqlDataType = "nvarchar(4000)"
                        break
                    }
                    "MultiSelectPicklist" {
                        $DataFlowDataType = "string"
                        $SqlDataType = "nvarchar(4000)"
                        $IsMultiSelectPicklist = $true
                        $TruncateColumn = $true
                        break
                    }
                    "ManagedProperty" {
                        $DataFlowDataType = "string"
                        $SqlDataType = "nvarchar(4000)"
                        $TruncateColumn = $true
                        break
                    }
                    "File" {
                        $DataFlowDataType = "string"
                        $SqlDataType = "nvarchar(4000)"
                        $TruncateColumn = $true
                        break
                    }
                    Default {
                        Write-Warning "Table $TableName - Column $ColumnName - Unknown string datatype $($DataLakeMetadataAttributes[$ColumnName].AttributeType)"
                        $DataFlowDataType = "string"
                        $SqlDataType = "nvarchar(4000)"
                        $TruncateColumn = $true
                    }
                    
                }
                break
            }
            Default {
                Write-Warning "Table $TableName - Column $ColumnName - Unknown data type $($DataLakeModelColumn.dataType), defaulting to string"
                $DataFlowDataType = "string"
                $SqlDataType = "nvarchar(4000)"
                $TruncateColumn = $true
            }
        }

        $ReferenceTypeAttribute = "$($ColumnName)_entitytype"
        if ($ReferenceTypeAttribute -notin $DataLakeMetadataAttributes.Keys) {
            $ReferenceTypeAttribute = $null
        }

        $Column = [PSCustomObject]@{
            TableName              = $TableName;
            Name                   = $ColumnName;
            DataFlowDataType       = $DataFlowDataType;
            SqlDataType            = $SqlDataType;
            References             = $ColumnReferences[$ColumnName];
            ReferenceTypeAttribute = $ReferenceTypeAttribute;
            HasOptionSet           = ($DataLakeModelColumn.dataType -ne "boolean") -and $OptionSets -and $OptionSets.Contains($ColumnName);
            IsMultiSelectPicklist  = $IsMultiSelectPicklist;
            TruncateColumn         = $TruncateColumn
        }

        if ($Column.HasOptionSet) {
            $Table.HasOptionSets = $true
        }

        $Column
    }

    $Table
}

function Format-TransformationName {
    param (
        $Name
    )

    $Name -replace "[^a-zA-Z0-9]+", ""
}

function Get-SourceTransformationName {
    param (
        $TableName
    )

    Format-TransformationName "Source$((Get-Culture).TextInfo.ToTitleCase($TableName))"
}

function Get-LookupDeriveTransformationName {
    param (
        $TableName
    )

    Format-TransformationName ("Derive{0}EntityType" -f $TableName)
}

function Get-CsvSourceTransformation {
    param (
        $Table,
        $LinkedService,
        $FileSystem
    )

    Write-Information "Generating CSV source transformation for table $($Table.Name)"

    $SourceTransformationName = Get-SourceTransformationName $Table.Name

    # {0} = $OutputColumns
    # {1} = $FileSystem
    # {2} = $TableName
    # {3} = $SourceTransformationName
    $SourceTemplate = "
    source(output(
    {0}
        ),
        fileSystem: '{1}',
        wildcardPaths:['{2}/*.csv'],
        allowSchemaDrift: true,
        validateSchema: false,
        ignoreNoFilesFound: true,
        inferDriftedColumnTypes: true,
        format: 'delimited',
        columnDelimiter: ',',
        escapeChar: '\`"',
        qutoChar: '\`"',
        columnNamesAsHeader: false,
        multiLineRow: true,
        timestampFormats: [
            'MM/dd/yyyy HH:mm:ss',
            'MM/dd/yyyy hh:mm:ss a',
            'MM-dd-yyyy HH:mm:ss',
            'MM-dd-yyyy hh:mm:ss a',
            'yyyy.MM.dd HH:mm:ss',
            'yyyy.MM.dd hh:mm:ss a',
            'yyyy-MM-dd\'T\'HH:mm:ss',
            'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'',
            'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''
        ]) ~> {3} 
    "

    $TableName = $Table.Name

    $OutputColumns = [String]::Join(",`n", 
        $(
            foreach ($Column in $Table.Columns) {
                "        {$($Column.Name)} as $($Column.DataFlowDataType)"
            }
        )
    )

    $Script = $SourceTemplate -f $OutputColumns, $FileSystem, $TableName, $SourceTransformationName

    [PSCustomObject]@{
        Name          = $SourceTransformationName;
        Script        = $Script;
        LinkedService = $LinkedService
    }
}

function Get-CsvSinkTransformation {
    param (
        $SourceName,
        $LinkedService
    )

    # {0} = $SourceName
    $SinkTemplate = "
        {0} sink(allowSchemaDrift: true,
        validateSchema: false,
        format: 'delimited',
        fileSystem: 'test',
        folderPath: 'output',
        columnDelimiter: ',',
        escapeChar: '\`"',
        qutoChar: '\`"',
        columnNamesAsHeader: true,
        partitionBy('hash', 1),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true,
        quoteAll: true) ~> sink1 
    "

    $Script = $SinkTemplate -f $SourceName

    [PSCustomObject]@{
        Name          = "sink1";
        Script        = $Script;
        LinkedService = $LinkedService
    }
}

function Add-OptionSetSourceTransformations {
    param (
        $DataFlow,
        $TableName,
        $LinkedService,
        $FileSystem
    )

    Write-Information "Adding source transformation for OptionSet fields"

    $SourceName = Format-TransformationName "Source$($TableName)Metadata"

    $Source = [PSCustomObject]@{
        Name          = $SourceName;
        LinkedService = $LinkedService;
        Script        = "
            source(output(
                AttributeMetadata as (AttributeName as string, AttributeType as string, AttributeTypeCode as short, EntityName as string, MetadataId as string, Precision as short, Timestamp as string, Version as integer)[],
                GlobalOptionSetMetadata as (IsUserLocalizedLabel as boolean, LocalizedLabel as string, LocalizedLabelLanguageCode as short, Option as integer, OptionSetName as string)[],
                OptionSetMetadata as (EntityName as string, IsUserLocalizedLabel as boolean, LocalizedLabel as string, LocalizedLabelLanguageCode as short, Option as short, OptionSetName as string)[],
                StateMetadata as (EntityName as string, IsUserLocalizedLabel as boolean, LocalizedLabel as string, LocalizedLabelLanguageCode as short, State as boolean)[],
                StatusMetadata as (EntityName as string, IsUserLocalizedLabel as boolean, LocalizedLabel as string, LocalizedLabelLanguageCode as short, State as boolean, Status as short)[],
                TargetMetadata as (AttributeName as string, EntityName as string, ReferencedAttribute as string, ReferencedEntity as string)[]
            ),
                allowSchemaDrift: true,
                validateSchema: false,
                ignoreNoFilesFound: false,
                format: 'json',
                fileSystem: '{0}',
                folderPath: 'Microsoft.Athena.TrickleFeedService',
                fileName: '{1}-EntityMetadata.json',
                documentForm: 'singleDocument') ~> {2} 
            " -f $FileSystem, $TableName, $SourceName
    }    
    $DataFlow.Sources[$Source.Name] = $Source

    $FlattenGlobalOptionSets = [PSCustomObject]@{
        Name   = "FlattenGlobalOptionSets";
        Script = "
            {0} foldDown(unroll(GlobalOptionSetMetadata),
            mapColumn(
                OptionSetName = GlobalOptionSetMetadata.OptionSetName,
                Option = GlobalOptionSetMetadata.Option,
                LocalizedLabel = GlobalOptionSetMetadata.LocalizedLabel
            ),
            skipDuplicateMapInputs: false,
            skipDuplicateMapOutputs: false) ~> FlattenGlobalOptionSets 
            " -f $SourceName
    }
    $DataFlow.Transformations += @($FlattenGlobalOptionSets)

    $FlattenOptionSets = [PSCustomObject]@{
        Name   = "FlattenOptionSets";
        Script = "
            {0} foldDown(unroll(OptionSetMetadata),
            mapColumn(
                OptionSetName = OptionSetMetadata.OptionSetName,
                Option = OptionSetMetadata.Option,
                LocalizedLabel = OptionSetMetadata.LocalizedLabel
            ),
            skipDuplicateMapInputs: false,
            skipDuplicateMapOutputs: false) ~> FlattenOptionSets 
            " -f $SourceName
    }
    $DataFlow.Transformations += @($FlattenOptionSets)

    $UnionOptionSets = [PSCustomObject]@{
        Name   = "UnionOptionSets";
        Script = "FlattenOptionSets, FlattenGlobalOptionSets union(byName: true) ~> UnionOptionSets "
    }
    $DataFlow.Transformations += @($UnionOptionSets)

    $CacheOptionSets = [PSCustomObject]@{
        Name   = "CacheOptionSets";
        Script = "
            UnionOptionSets sink(skipDuplicateMapInputs: true,
                skipDuplicateMapOutputs: true,
                keys:['OptionSetName','Option'],
                store: 'cache',
                format: 'inline',
                output: false,
                saveOrder: 1) ~> CacheOptionSets 
            "
    }
    $DataFlow.Sinks += @($CacheOptionSets)

}

function Add-OptionSetDerivedColumnTransformation {
    param(
        $DataFlow,
        $Columns
    )

    Write-Information "Adding derive transformation for OptionSet fields"

    $DeriveTransformationName = Format-TransformationName "DeriveOptionSetColumns"

    $ColumnScripts = foreach ($Column in $Columns) {
        if ($Column.IsMultiSelectPicklist) {
            "substring(reduce(
                split({0},`";`"),
                `"`",
                #acc + `";`" + toString(SinkCache#lookup(`"{0}`", toInteger(#item)).LocalizedLabel),
                #result),2)" -f $Column.Name
        }
        else {
            "{0} = CacheOptionSets#lookup(`"{0}`", toInteger({0})).LocalizedLabel" -f $Column.Name
        }
    }

    $Script = "
    {0} derive(
        {1}
    ) ~> {2} 
    " -f $DataFlow.Root.Name, [string]::Join(",`n        ", $ColumnScripts), $DeriveTransformationName

    $DeriveTransformation = [PSCustomObject]@{
        Name   = $DeriveTransformationName;
        Script = $Script
    }

    $DataFlow.Transformations += @($DeriveTransformation)
    $DataFlow.Root = $DeriveTransformation
}

function Add-LookupColumn {
    param (
        $DataFlow,
        $Column
    )

    Write-Information "Adding lookup column $($Column.Name)"

    $Selects = @()

    $DescriptionColumnName = "$($Column.Name)_description"

    foreach ($Reference in $Column.References) {
        $SourceName = Get-LookupDeriveTransformationName $Reference.ReferencedEntity

        $SelectName = Format-TransformationName ("Select{0}{1}" -f `
                $Column.Name, `
                $Reference.ReferencedEntity)

        $SelectScript = " {0} select(mapColumn(
                {1}_entitytype2 = entitytype,
                {1} = {2},
                {5} = {3}                
            ),
            skipDuplicateMapInputs: true,
            skipDuplicateMapOutputs: true) ~> {4} " -f `
            $SourceName, `
            $Column.Name, `
            $Reference.ReferencedAttribute, `
            $Reference.ReferencedEntityNameAttribute, `
            $SelectName, `
            $DescriptionColumnName

        $Select = [PSCustomObject]@{
            Name   = $SelectName;
            Script = $SelectScript
        }

        $DataFlow.Transformations += @($Select)

        $Selects += @($Select.Name)
    }

    if ($Selects.Length -gt 1) {
        $UnionName = Format-TransformationName ("Union{0}" -f $Column.Name)

        $UnionScript = "{0} union(byName: true)~> {1} " -f `
            [string]::Join(", ", $Selects), `
            $UnionName

        $Union = [PSCustomObject]@{
            Name   = $UnionName;
            Script = $UnionScript
        }

        $DataFlow.Transformations += @($Union)

        $LookupSource = $UnionName
    }
    else {
        $LookupSource = $Selects[0]
    }

    $LookupName = Format-TransformationName ("Lookup{0}" -f $Column.Name)

    $LookupScript = "
        {0}, {1} lookup(
            {5}@{2} == {1}@{2}
            {3}
            multiple: false,
            pickup: 'any',
            broadcast: 'auto') ~> {4} " -f `
        $DataFlow.Root.Name, `
        $LookupSource, `
        $Column.Name, `
    $(
        if ($Column.ReferenceTypeAttribute) {
            "&& {1} == {3}_entitytype2," -f `
            $(Get-SourceTransformationName $Column.TableName), `
                $Column.ReferenceTypeAttribute, `
                $LookupSource, `
                $Column.Name
        }
        else {
            ""
        }
    ), `
        $LookupName,
    $(Get-SourceTransformationName $Column.TableName)

    $Lookup = [PSCustomObject]@{
        Name   = $LookupName;
        Script = $LookupScript
    }

    $DataFlow.Transformations += @($Lookup)
    $DataFlow.Root = $Lookup
    
    [PSCustomObject]@{
        Name             = $DescriptionColumnName;
        SourceColumn     = "{0}" -f `
            $DescriptionColumnName;
        SqlDataType      = $Column.SqlDataType;
        DataFlowDataType = $Column.DataFlowDataType
    }
}
function Add-TruncateColumnsDeriveTransformation {
    param (
        $DataFlow,
        $ColumnsToTruncate
    )

    Write-Information "Adding derive transformation for column truncation"

    $DeriveName = Format-TransformationName "DeriveTruncateColumns"

    $DeriveScript = "{0} derive(
        {1}
        ) ~> {2} " -f `
        $DataFlow.Root.Name, `
        [String]::Join(",\r        ", $(
            foreach ($Column in $ColumnsToTruncate) {
                "{0} = left({0},4000)" -f $Column
            }
        )), `
        $DeriveName

    $Derive = [PSCustomObject]@{
        Name   = $DeriveName;
        Script = $DeriveScript
    }

    $DataFlow.Transformations += @($Derive)

    $DataFlow.Root = $Derive
}

function Add-OutputSelectTransformation {
    param (
        $DataFlow,
        $OutputColumns
    )

    Write-Information "Adding output select transformation"

    $Columns = [string]::Join(",`n        ", 
        $($OutputColumns | ForEach-Object { "$($_.Name) = $($_.SourceColumn)" })
    )

    $SelectName = Format-TransformationName "SelectOutput"

    $SelectScript = "{0} select(mapColumn(
            {1}
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> {2} " -f `
        $DataFlow.Root.Name, `
        $Columns, `
        $SelectName

    $Select = [PSCustomObject]@{
        Name   = $SelectName;
        Script = $SelectScript
    }

    $DataFlow.Transformations += @($Select)

    $DataFlow.Root = $Select
}

function Add-SqlSink {
    param (
        $DataFlow,
        $DataSet,
        $OutputColumns
    )

    Write-Information "Adding SQL sink"

    $SinkName = "SinkOutput"

    $SinkScript = "{0} sink(input(
        {1}
        ),
        allowSchemaDrift: true,
        validateSchema: false,
        deletable:false,
        insertable:true,
        updateable:false,
        upsertable:false,
        format: 'table',
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true,
        errorHandlingOption: 'stopOnFirstError') ~> {2} " -f `
        $DataFlow.Root.Name, `
        [String]::Join(",\r        ", $(
            foreach ($Column in $OutputColumns) {
                "{0} as {1}" -f `
                    $Column.Name, `
                    $Column.DataFlowDataType
            }
        )), `
        $SinkName

    $DataFlow.Sinks += @([PSCustomObject]@{
            Name    = $SinkName;
            Script  = $SinkScript;
            DataSet = $DataSet
        })
}

function Get-DataFlowCode {
    param (
        $DataFlow
    )

    Write-Information "Generating code for data flow $($DataFlow.name)"

    $DataFlowObj = @{}
    $DataFlowObj.name = $DataFlow.name

    $TransformationScripts = $DataFlow.Sources.Values | ForEach-Object { $_.Script }
    $TransformationScripts += $DataFlow.Transformations | ForEach-Object { $_.Script }
    $TransformationScripts += $DataFlow.Sinks | ForEach-Object { $_.Script }

    $DataFlowObj.properties = @{
        type           = "MappingDataFlow";
        typeProperties = @{
            sources         = @(
                $DataFlow.Sources.Values | ForEach-Object {
                    @{
                        name          = $_.Name;
                        linkedService = @{
                            referenceName = $_.LinkedService;
                            type          = "LinkedServiceReference"
                        }
                    }
                }
            );
            transformations = @(
                $DataFlow.Transformations | ForEach-Object {
                    @{
                        name = $_.Name
                    }
                }
            );
            sinks           = @(
                $DataFlow.Sinks | ForEach-Object {
                    $Sink = @{
                        name = $_.Name;
                    }
                    if ($_.LinkedService) {
                        $Sink.linkedService = @{
                            referenceName = $_.LinkedService;
                            type          = "LinkedServiceReference"
                        }
                    }
                    if ($_.DataSet) {
                        $Sink.dataset = @{
                            referenceName = $_.DataSet;
                            type          = "DatasetReference"
                        }
                    }
                    $Sink
                }                
            );
            script          = [String]::Join(" ", $TransformationScripts)
        }
    }

    $DataFlowObj | ConvertTo-Json -Depth 100
}

function Build-TableScript {
    param (
        $TableName,
        $Columns
    )

    Write-Information "Generating script for table $TableName"

    $TableScript = "
    CREATE TABLE [{0}] (
        {1}
    )
    " -f `
        $TableName, `
        [String]::Join(",`n", $(
            foreach ($Column in $Columns) {
                "[{0}] {1}" -f `
                    $Column.Name, `
                    $Column.SqlDataType
            }
        ))
        
    $TableScript | Out-File "$($Config.ArtefactsPath)\$TableName.sql"
}

function Build-TableArtefacts {
    param(
        $TableName,
        $DataSet,
        $DataLakeLinkedService,
        $FileSystem
    )

    Write-Information "Building data flow artefacts for table $TableName"

    $DataFlow = @{}
    $DataFlow.Name = "DataFlow$((Get-Culture).TextInfo.ToTitleCase($TableName))"
    $DataFlow.Sources = @{}

    $Table = Get-Table $TableName -ExpandReferences
    $MainTableSource = Get-CsvSourceTransformation `
        -Table $Table `
        -LinkedService $DataLakeLinkedService `
        -FileSystem $FileSystem
    $DataFlow.Sources[$Table.Name] = $MainTableSource
    $DataFlow.Root = $MainTableSource

    if ($Table.HasOptionSets) {
        Add-OptionSetSourceTransformations `
            -DataFlow $DataFlow `
            -TableName $TableName `
            -LinkedService $DataLakeLinkedService `
            -FileSystem $FileSystem
    }

    foreach ($ReferencedEntityName in $Table.ReferencedEntities) {
        $ReferencedTable = Get-Table $ReferencedEntityName
        $ReferencedTableSource = Get-CsvSourceTransformation `
            -Table $ReferencedTable `
            -LinkedService $DataLakeLinkedService `
            -FileSystem $FileSystem
        $DataFlow.Sources[$ReferencedEntityName] = $ReferencedTableSource

        $DeriveName = Get-LookupDeriveTransformationName $ReferencedEntityName

        $DeriveScript = "
        {0} derive(entitytype = `"{1}`") ~> {2} " -f `
            $ReferencedTableSource.Name, `
            $ReferencedEntityName, `
            $DeriveName

        $Derive = [PSCustomObject]@{
            Name   = $DeriveName;
            Script = $DeriveScript
        }

        $DataFlow.Transformations += @($Derive)
    }

    $OptionSetDerivedColumns = @()
    $ColumnsToTruncate = @()
    $OutputColumns = @()

    foreach ($Column in $Table.Columns) {
        if ($Column.TruncateColumn) {
            $ColumnsToTruncate += @($Column.Name)
        }

        if ($Column.HasOptionSet) {
            $OptionSetDerivedColumns += @($Column)
            $OutputColumns += @(
                [PSCustomObject]@{
                    Name             = $Column.Name;
                    SourceColumn     = "{0}" -f `
                        $Column.Name;
                    SqlDataType      = "nvarchar(4000)";
                    DataFlowDataType = "string"
                }
            )
            $ColumnsToTruncate += @($Column.Name)
        }
        else {
            if ($Column.References) {
                $OutputColumns += @(
                    [PSCustomObject]@{
                        Name             = $Column.Name;
                        SourceColumn     = "{1}" -f `
                            $MainTableSource.Name, `
                            $Column.Name;
                        SqlDataType      = $Column.SqlDataType;
                        DataFlowDataType = $Column.DataFlowDataType
                    }
                )    

                $LookupColumn = Add-LookupColumn `
                    -DataFlow $DataFlow `
                    -Column $Column
                
                $OutputColumns += @($LookupColumn)
            } 
            else {
                $OutputColumns += @(
                    [PSCustomObject]@{
                        Name             = $Column.Name;
                        SourceColumn     = "{0}" -f `
                            $Column.Name;
                        SqlDataType      = $Column.SqlDataType;
                        DataFlowDataType = $Column.DataFlowDataType
                    }
                )    
            }
        }
    }

    if ($OptionSetDerivedColumns) {
        Add-OptionSetDerivedColumnTransformation `
            -DataFlow $DataFlow `
            -Columns $OptionSetDerivedColumns
    }

    if ($ColumnsToTruncate) {
        Add-TruncateColumnsDeriveTransformation `
            -DataFlow $DataFlow `
            -ColumnsToTruncate $ColumnsToTruncate
    }

    Add-OutputSelectTransformation `
        -DataFlow $DataFlow `
        -OutputColumns $OutputColumns

    Add-SqlSink `
        -DataFlow $DataFlow `
        -DataSet $DataSet `
        -OutputColumns $OutputColumns

    $DataFlowCode = Get-DataFlowCode $DataFlow

    if (-not (Test-Path -Path $Config.ArtefactsPath)) {
        mkdir $Config.ArtefactsPath | Out-Null
    }

    $DataFlowCode | Out-File "$($Config.ArtefactsPath)\$($DataFlow.Name).json"

    Build-TableScript `
        -TableName $TableName `
        -Columns $OutputColumns
}

$Config.TablesToIngest | ForEach-Object {
    Build-TableArtefacts `
        -TableName $_.Name `
        -DataSet $_.DataSet `
        -DataLakeLinkedService $Config.DataLakeLinkedService `
        -FileSystem $Config.FileSystem
}
