import grpc
import rest_to_main_pb2
import rest_to_main_pb2_grpc
from fastapi import FastAPI
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse


app = FastAPI()

# Function for chunking file to send over gRPC
# The full file is breaken down into chunks and each chunk is then further broken down and iterated over 
async def generate_file_chunks(file: UploadFile, filename: str, chunk_size=1024 * 1024):
    chunk_number = 0
    while True:
        chunk = await file.read(chunk_size)
        if not chunk:
            break
        yield rest_to_main_pb2.transferRequest(
            data=chunk,
            chunk_number=chunk_number,
            filename=filename if chunk_number == 0 else ""
        )
        chunk_number += 1

# client -> server POST Req API Endpt:
# send .txt file 
# make grpc call to send the .txt file to node 
@app.post("/upload-txt")
async def upload_txt(file: UploadFile = File(...)):
    if not file.filename.endswith(".txt"):
        return JSONResponse(content={"error": "Only .txt files are allowed"}, status_code=400)
    
    # TODo: localhost needs to b changed to master IP when we have multiple nodes
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = rest_to_main_pb2_grpc.TransferFileServiceStub(channel)
        
        response = await stub.transferFile(generate_file_chunks(file, file.filename))
        
        if response.success:
            return {"message": response.status_message}
        else:
            return JSONResponse(content={"error": response.status_message}, status_code=500)
        
# client -> server GET Req API Endpt:
# send .txt file name and file is fetched and downloaded
@app.get("/download-txt/{filename}")
async def download_txt(filename: str):
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = rest_to_main_pb2_grpc.TransferFileServiceStub(channel)
        
        request = rest_to_main_pb2.FileRequest(filename=filename)
        response = await stub.getFile(request)  
        
        file_content = BytesIO()
        
        async for chunk in response:
            file_content.write(chunk.data) 
        
        file_content.seek(0)  

        return StreamingResponse(file_content, media_type="text/plain", headers={"Content-Disposition": f"attachment; filename={filename}"})




# Master Node -> backend post request to update new master