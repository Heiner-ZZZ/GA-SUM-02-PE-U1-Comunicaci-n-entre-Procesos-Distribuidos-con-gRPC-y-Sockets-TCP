@echo off
python -m grpc_tools.protoc -Iproto --python_out=src/grpc --grpc_python_out=src/grpc proto/messaging.proto
if %errorlevel% neq 0 (
    echo Error al compilar proto/messaging.proto
    exit /b %errorlevel%
)
echo Stubs gRPC generados correctamente en src/grpc/
