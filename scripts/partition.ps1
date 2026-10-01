# Odpojenie / pripojenie uzla od spolocnej siete klastra.
#   .\scripts\partition.ps1 disconnect b
#   .\scripts\partition.ps1 connect b
param(
  [Parameter(Mandatory)][ValidateSet("disconnect","connect")] [string]$Action,
  [Parameter(Mandatory)][ValidateSet("a","b","c")] [string]$Node
)
$container = "dt-api-$Node"
if ($Action -eq "disconnect") {
  docker network disconnect dt_cluster $container
} else {
  docker network connect --alias "api-$Node" dt_cluster $container
}
docker network inspect dt_cluster --format "Siet dt_cluster: {{range .Containers}}{{.Name}} {{end}}"
