$ErrorActionPreference = "Continue"

$baseUrl = "http://127.0.0.1:8001"
$timezoneName = "Europe/Madrid"
$pollSeconds = 60

$headers = @{
    "Content-Type" = "application/json"
    "X-EYE-Timezone" = $timezoneName
}

$signalPayloads = @(
    @{ symbol = "NDX"; timeframe = "1h" },
    @{ symbol = "WTI"; timeframe = "1h" }
)

function Invoke-EyePost {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [Parameter(Mandatory = $true)][hashtable]$Body
    )

    try {
        $jsonBody = $Body | ConvertTo-Json -Depth 10
        $response = Invoke-RestMethod -Method Post -Uri $Url -Headers $headers -Body $jsonBody
        return $response
    }
    catch {
        Write-Host "[ERROR] $Url -> $($_.Exception.Message)" -ForegroundColor Red
        return $null
    }
}

Write-Host "EYE live launcher avviato" -ForegroundColor Cyan
Write-Host "Base URL: $baseUrl"
Write-Host "Timezone: $timezoneName"
Write-Host "Polling: ${pollSeconds}s"
Write-Host ""

while ($true) {
    $now = Get-Date
    Write-Host "[$($now.ToString('yyyy-MM-dd HH:mm:ss'))] ciclo EYE..." -ForegroundColor DarkGray

    # 1) Briefing automatici: decide il runner se lo slot e dovuto
    $briefingResult = Invoke-EyePost -Url "$baseUrl/api/v1/briefings/run" -Body @{}
    if ($null -ne $briefingResult) {
        Write-Host ("  briefing -> " + ($briefingResult | ConvertTo-Json -Compress))
    }

    # 2) Trigger segnali reali: il backend decide se inviare update/alert
    foreach ($payload in $signalPayloads) {
        $signalResult = Invoke-EyePost -Url "$baseUrl/api/v1/signals/generate" -Body $payload
        if ($null -ne $signalResult) {
            $asset = $signalResult.asset
            $action = $signalResult.action
            $confidence = $signalResult.confidence_label
            Write-Host "  signal -> $asset | action=$action | confidence=$confidence"
        }
    }

    Start-Sleep -Seconds $pollSeconds
}
