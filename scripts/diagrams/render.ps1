param()

$ErrorActionPreference = 'Stop'
$diagramRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../../docs/diagrams'))
$diagramConfig = Join-Path $diagramRoot 'mermaid-config.json'
$diagramNpx = (Get-Command npx -ErrorAction Stop).Source

foreach ($diagramSource in Get-ChildItem -LiteralPath $diagramRoot -Filter '*.mmd' -File) {
    $diagramOutput = [System.IO.Path]::ChangeExtension($diagramSource.FullName, '.svg')
    & $diagramNpx --yes '@mermaid-js/mermaid-cli@11.17.0' `
        --input $diagramSource.FullName --output $diagramOutput `
        --configFile $diagramConfig --backgroundColor white --width 1200
    if ($LASTEXITCODE -ne 0) {
        throw "Diagram render failed for $($diagramSource.Name)"
    }
}
