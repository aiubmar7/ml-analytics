# ============================================================
#  Costo de envíos ME2 drop-off del VENDEDOR — Noviembre 2026
#  Regla validada: mode=='me2' y shipping_option.cost==0  =>  list_cost
#                  ME1 / comprador paga el envío           =>  0
#  Lee el token del Dropbox local (no lo expone en pantalla).
# ============================================================

$tokFile = "C:\Users\Aiubm\Dropbox\ml_analytics\tokens\ml_tokens.json"
$tok = (Get-Content $tokFile -Raw | ConvertFrom-Json).access_token
$h   = @{ Authorization = "Bearer $tok" }

# --- user_id del vendedor ---
$me  = Invoke-RestMethod "https://api.mercadolibre.com/users/me" -Headers $h
$uid = $me.id

# --- ventana noviembre 2026 (hora Uruguay) ---
$from = "2026-11-01T00:00:00.000-03:00"
$to   = "2026-11-30T23:59:59.000-03:00"

Write-Host "Bajando ordenes de noviembre..." -ForegroundColor Cyan

# --- paginar /orders/search y juntar shipping ids unicos ---
$shipIds = @{}   # shipId -> order_id (para dedup; 1 envio por orden)
$offset  = 0
do {
    $url  = "https://api.mercadolibre.com/orders/search?seller=$uid&sort=date_desc&order.date_created.from=$from&order.date_created.to=$to&offset=$offset&limit=50"
    $page = Invoke-RestMethod $url -Headers $h
    foreach ($o in $page.results) {
        $sid = $o.shipping.id
        if ($sid) { $shipIds[[string]$sid] = $o.id }
    }
    $total   = $page.paging.total
    $offset += 50
    Write-Host ("  ordenes: {0}/{1}" -f [math]::Min($offset,$total), $total)
} while ($offset -lt [math]::Min($total, 9950))

Write-Host ("Envios unicos a consultar: {0}" -f $shipIds.Count) -ForegroundColor Cyan

# --- consultar cada envio y aplicar la regla ---
$totEnvio = 0.0
$nPaga    = 0      # ME2 gratis -> vendedor paga
$nME1     = 0      # excluidos
$nComprador = 0    # comprador pago
$nErr     = 0
$i = 0
foreach ($sid in $shipIds.Keys) {
    $i++
    if ($i % 100 -eq 0) { Write-Host ("  envios: {0}/{1}" -f $i, $shipIds.Count) }
    try {
        $s = Invoke-RestMethod "https://api.mercadolibre.com/shipments/$sid" -Headers $h
    } catch { $nErr++; continue }

    if ($s.mode -ne "me2") { $nME1++; continue }
    $buyer = [double]($s.shipping_option.cost)
    $listc = [double]($s.shipping_option.list_cost)
    if ($buyer -eq 0) { $totEnvio += $listc; $nPaga++ }
    else             { $nComprador++ }
}

Write-Host ""
Write-Host "===== RESULTADO ENVIOS NOVIEMBRE =====" -ForegroundColor Green
Write-Host ("Ordenes con envio                : {0}" -f $shipIds.Count)
Write-Host ("  ME2 gratis (vendedor paga)     : {0}" -f $nPaga)
Write-Host ("  Comprador pago el envio        : {0}" -f $nComprador)
Write-Host ("  ME1 / otro (excluidos)         : {0}" -f $nME1)
Write-Host ("  Errores de consulta            : {0}" -f $nErr)
Write-Host ""
Write-Host ("COSTO TOTAL ENVIOS (vendedor)    : $ {0:N0}" -f $totEnvio) -ForegroundColor Yellow
if ($nPaga -gt 0) {
    Write-Host ("Promedio por envio pago          : $ {0:N0}" -f ($totEnvio / $nPaga))
}
