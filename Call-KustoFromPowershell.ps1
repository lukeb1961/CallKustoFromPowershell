# Call-KustoFromPowershell.ps1

PARAM ([Parameter(Mandatory=$true)] [string]$ClusterName,
       [Parameter(Mandatory=$true)] [string]$DatabaseName,
       [Parameter(Mandatory=$true)] [string]$ArgName,
       [Parameter(Mandatory=$true)] [string]$ArgValue
       )


#region ImportKustoDotNetPackages 

# Microsoft.Azure.Kusto.Tools 7.0.0 or higher work fine from PS 5 and PWSH 7 and even loads in WSL Ubuntu\Linux.
# use Find-Package Microsoft.Azure.Kusto.Tools | Install-Package

$PSver=$PSVersionTable.PSVersion.Major ## Pwsh 7 uses dotnet<N> but PS 5 still uses dotnet 4.7
try {
 $PackageToolsDir = Join-Path -Path (Split-Path -Parent (Get-Package -Name 'Microsoft.Azure.Kusto.Tools' -Provider NuGet -EA SilentlyContinue).source) -ChildPath 'tools'
}
catch {
 Write-Error 'Package Not Found. Install via Find-Package Microsoft.Azure.Kusto.Tools | Install-Package'
}
#decide which dotnet version to load
if ($PSver -le 5) {  
        $DotNetDir = Get-ChildItem  -path $PackageToolsDir -Directory -Name 'Net*' | Sort-Object | Select-Object -First 1 }
else {  $DotNetDir = Get-ChildItem  -path $PackageToolsDir -Directory -Name 'Net*' | Sort-Object -Descending | Select-Object -First 1 }

$PackageDotNet     = Join-Path -Path $PackageToolsDir -ChildPath $DotNetDir

# manually load in case there is no default path (eg in WSL Ubuntu)
$PKG1=[System.Reflection.Assembly]::LoadFrom((Join-Path $PackageDotNet 'Kusto.Data.dll'))
$PKG2=[System.Reflection.Assembly]::LoadFrom((Join-Path $PackageDotNet 'Kusto.Cloud.Platform.dll'))
if (Test-Path (Join-Path $PackageDotNet 'Kusto.Cloud.Platform.Aad.dll')) {
 $PKG3=[System.Reflection.Assembly]::LoadFrom((Join-Path $PackageDotNet 'Kusto.Cloud.Platform.Aad.dll'))
}
if (Test-Path (Join-Path $PackageDotNet 'Kusto.Cloud.Platform.Msal.dll')) {
 $PKG3=[System.Reflection.Assembly]::LoadFrom((Join-Path $PackageDotNet 'Kusto.Cloud.Platform.Msal.dll'))
}
$PKG4=[System.Reflection.Assembly]::LoadFrom((Join-Path $PackageDotNet 'Microsoft.Identity.Client.dll'))
$PKG5=[System.Reflection.Assembly]::LoadFrom((Join-Path $PackageDotNet 'Microsoft.IdentityModel.Abstractions.dll'))
$PKG6=[System.Reflection.Assembly]::LoadFrom((Join-Path $PackageDotNet 'Microsoft.Identity.Client.Extensions.Msal.dll'))

#endregion

#region InvokeKustoQuery
function Invoke-KustoQuery {
  PARAM ([Parameter(Mandatory=$true)][string] $Cluster, 
         [Parameter(Mandatory=$true)][string] $DataBaseName, 
         [Parameter(Mandatory=$true)][string] $query,
         [Parameter(Mandatory=$true)][string] $argName,
         [Parameter(Mandatory=$true)][string] $argValue
         )

  $BindingFlags= [Reflection.BindingFlags] 'NonPublic,Static'

  # define which cluster and database to connect to
  $clusterUrl = ('https://{0}.kusto.windows.net:443;AAD Federated Security=True' -f $Cluster)
  $kcsb = New-Object -TypeName Kusto.Data.KustoConnectionStringBuilder -ArgumentList ($clusterUrl, $DataBaseName)
  $client = [Kusto.Data.Net.Client.KustoClientFactory]::CreateCslQueryProvider($kcsb)  # from Kusto.Data.dll

  # set the server timeout property
  $crp = New-Object -TypeName Kusto.Data.Common.ClientRequestProperties   # from Kusto.Data.dll
  $crp.ClientRequestId = 'PSScript.ExecuteQuery.' + [Guid]::NewGuid().ToString()
  $crp.SetOption([Kusto.Data.Common.ClientRequestProperties]::OptionServerTimeout, [TimeSpan]::FromSeconds(30))
  $crp.SetParameter($argName,$argValue)

  ## do the query and show results
  $reader = $client.ExecuteQuery($query, $crp)
  $dataTable = [Kusto.Cloud.Platform.Data.ExtendedDataReader]::ToDataSet($reader).Tables[0]  # from Kusto.Cloud.Platform.dll
  $dataView = New-Object -TypeName System.Data.DataView -ArgumentList ($dataTable)
  $dataView     
}
#endregion

#region ExampleKustoQueryToRun
$query = @'
declare query_parameters(KustoParam:string);
SomeTableNameHere
| where SomeColumnName == KustoParam
| sort by SomeColumnName asc 
| summarize take_any(*) by SomeColumnName 
'@
#endregion


$kustoResults = Invoke-KustoQuery -Cluster $ClusterName -DataBaseName $DatabaseName -query $query -argName $ArgName -argValue $ArgValue
$kustoResults
