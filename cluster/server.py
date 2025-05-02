import transfer_file_pb2
import transfer_file_pb2_grpc
import os
import grpc
import logging
import asyncio

class TransferFileServer(transfer_file_pb2_grpc.TransferFileServiceServicer):
    def __init__(self, dir_for_files):
        self.dir_for_files = dir_for_files

    def UploadFile(self, request_iterator, context):
        stream_iterator = iter(request_iterator) 
        try:
            stream_data = next(stream_iterator)
        except StopIteration:
            return transfer_file_pb2.UploadResponse(success=False, status_message="Could not parse stream")
        
        # First chunk receive should be 0, then we make the filepath off of the filename
        current_chunk = 0
        if stream_data.chunk_number == current_chunk:
            filename = stream_data.filename
            filepath = f"{self.dir_for_files}/{filename}"
        else:
            return transfer_file_pb2.UploadResponse(success=False, status_message="Chunk number not matching")

        # Open up a file in the OS and write, each time we write, we should update the chunk number to the next and check if its matching.
        with open(filepath, "wb") as file_pointer:
            file_pointer.write(stream_data.data)
            current_chunk += 1
            for remaining_chunk in stream_iterator:
                if (current_chunk != remaining_chunk.chunk_number):
                    return transfer_file_pb2.UploadResponse(success=False, status_message="Chunk number not matching")
                file_pointer.write(remaining_chunk.data)
                current_chunk += 1
        return transfer_file_pb2.UploadResponse(success=True, status_message="Stream parse finished")

    def DownloadFile(self, request, context):
        filename = request.filename
        filepath = f"{self.dir_for_files}/{filename}"
        chunk_number = 0
        chunk_size = 1024*1024
        with open(filepath, "rb") as file_pointer:
            while chunk := file_pointer.read(chunk_size):
                yield transfer_file_pb2.DownloadResponse(
                    data=chunk,
                    chunk_number=chunk_number,
                ) 
                chunk_number += 1

class ServerNode:
    def __init__(self):
        self.process_id = os.getpid()
        self.dir_for_files = f"""/tmp/{self.process_id}"""
        if not os.path.exists(self.dir_for_files):
            os.makedirs(self.dir_for_files)
        self.listen_addr = "[::]:50051"
        
    def print_dir_for_files(self) -> None:
        print(self.dir_for_files)

    async def serve(self):
        server = grpc.aio.server()
        transfer_file_pb2_grpc.add_TransferFileServiceServicer_to_server(TransferFileServer(self.dir_for_files), server)
        server.add_insecure_port(self.listen_addr)
        logging.info("Starting server on %s", self.listen_addr)
        await server.start()
        await server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(ServerNode().serve())
