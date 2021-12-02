Param
(
    [bool]$use_tls = $true,
    [bool]$use_winhttp = $false,
    [bool]$use_http3 = $false,
    [string]$framework = "net6.0",
    [string]$grpc_web_mode = "None",
    [int]$server_port = 443
)

$allTests =
  "large_unary",
  "server_streaming"

Write-Host "Running $($allTests.Count) tests" -ForegroundColor Cyan
Write-Host "Use TLS: $use_tls" -ForegroundColor Cyan
Write-Host "Use WinHttp: $use_winhttp" -ForegroundColor Cyan
Write-Host "Use HTTP/3: $use_http3" -ForegroundColor Cyan
Write-Host "Framework: $framework" -ForegroundColor Cyan
Write-Host "gRPC-Web mode: $grpc_web_mode" -ForegroundColor Cyan
Write-Host

for ($i = 0; $i -lt 100; $i++) {
  foreach ($test in $allTests)
  {
    Write-Host "Running $test" -ForegroundColor Cyan
  
    dotnet run --framework $framework --use_tls $use_tls --server_host grpc-test.sandbox.googleapis.com --server_port $server_port --client_type httpclient --test_case $test --use_winhttp $use_winhttp --grpc_web_mode $grpc_web_mode --use_http3 $use_http3
    $testExitCode = $LASTEXITCODE
  
    Write-Host "Test exit code: $testExitCode"
    Write-Host
  
    if ($testExitCode -ne 0)
    {
      exit $testExitCode
    }
  }
}


Write-Host "Done" -ForegroundColor Cyan