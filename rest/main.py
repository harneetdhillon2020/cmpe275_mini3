import grpc
import io
import transfer_file_pb2
import transfer_file_pb2_grpc
from fastapi import FastAPI, UploadFile, File, Body
from fastapi.responses import JSONResponse, StreamingResponse


app = FastAPI()

# Default Master Node IP
master_node_ip = "0.0.0.0:50051"

registry_ip = [ 
  master_node_ip,
  "0.0.0.0:50052",
  "0.0.0.0:50053",
  "0.0.0.0:50054",
  "0.0.0.0:50055"
]

# Function for chunking file to send over gRPC
# The full file is breaken down into chunks and each chunk is then further broken down and iterated over 
async def generate_file_chunks(file: UploadFile, filename: str, chunk_size=1024 * 1024):
    chunk_number = 0
    while True:
        chunk = await file.read(chunk_size)
        if not chunk:
            break
        yield transfer_file_pb2.UploadRequest(
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
    
    async with grpc.aio.insecure_channel(master_node_ip) as channel:
        stub = transfer_file_pb2_grpc.TransferFileServiceStub(channel)
        
        response = await stub.UploadFile(generate_file_chunks(file, file.filename))
        
        if response.success:
            return {"message": response.status_message}
        else:
            return JSONResponse(content={"error": response.status_message}, status_code=500)
        
# client -> server GET Req API Endpt:
# send .txt file name and file is fetched and downloaded
@app.get("/download-txt/{filename}")
async def download_txt(filename: str):
    async with grpc.aio.insecure_channel(master_node_ip) as channel:
        stub = transfer_file_pb2_grpc.TransferFileServiceStub(channel)
        
        request = transfer_file_pb2.DownloadRequest(filename=filename)
        response =  stub.DownloadFile(request)  
        
        file_content = io.BytesIO()

        async for chunk in response:
            file_content.write(chunk.data) 
        
        file_content.seek(0)  

        return StreamingResponse(file_content, media_type="text/plain", headers={"Content-Disposition": f"attachment; filename={filename}"})

# Master Get
@app.get("/master")
async def get_master_ip():
    return master_node_ip

# Master Node -> backend post request to update new master
@app.post("/master")
async def post_master_ip(ip: str = Body(..., embed=True)):
    global master_node_ip
    master_node_ip = ip
    return f"""New master node ip @ {master_node_ip}"""
    
@app.get("/registry") 
async def get_registry():
    return registry_ip
