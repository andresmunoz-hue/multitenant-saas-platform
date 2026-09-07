# Compila un winutils.exe no-op para Spark/Hadoop local en Windows.
# Hadoop invoca winutils para chmod; sin DLL nativas el binario oficial falla (0xC0000135).

$ErrorActionPreference = "Stop"
$bin = Join-Path $PSScriptRoot "..\.hadoop\bin"
New-Item -ItemType Directory -Force -Path $bin | Out-Null
$path = Join-Path $bin "winutils.exe"

$src = @"
using System;
public class WinUtils {
  public static int Main(string[] args) {
    return 0;
  }
}
"@

Add-Type -TypeDefinition $src -OutputAssembly $path -OutputType ConsoleApplication
Write-Host "Wrote $path"
