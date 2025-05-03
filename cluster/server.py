import heartbeat_pb2
import heartbeat_pb2_grpc
import transfer_file_pb2
import transfer_file_pb2_grpc

import argparse
import os
import grpc
import logging
import asyncio

class TransferFileService(transfer_file_pb2_grpc.TransferFileServiceServicer):
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

class HeartBeatService(heartbeat_pb2_grpc.HeartBeatServiceServicer):
    def HeartBeat(self, request, context):
        print("Heartbeat from: ", request.pid, "storage in MB: ", request.storage)
        return heartbeat_pb2.HeartBeatResponse(status=True)

class ServerNode:
    def __init__(self, port, rest_ip_addr):

        # Initialized Immutable Fields. These should not be changed, denoted by _ prefix.
        self._process_id = os.getpid()
        self._listen_addr = "0.0.0.0"
        self._listen_port = port 
        self._rest_ip_addr = rest_ip_addr
        self._dir_for_files = f"""/tmp/{self._process_id}-{self._listen_port}"""
        if not os.path.exists(self._dir_for_files):
            os.makedirs(self._dir_for_files)

        # Mutable Parameters
        # Use a lock to when dealing with setting/getting these state fields. Just in case of weird race conditions.
        self._master_lock = asyncio.Lock()
        self.is_master: bool = False
        self.master_addr: str = self._listen_addr
        self.master_port: int = self._listen_port               


    def print_dir_for_files(self) -> None:
        print(self._dir_for_files)

    async def serve(self):
        server = grpc.aio.server()
        transfer_file_pb2_grpc.add_TransferFileServiceServicer_to_server(TransferFileService(self._dir_for_files), server)
        heartbeat_pb2_grpc.add_HeartBeatServiceServicer_to_server(HeartBeatService(), server)
        server.add_insecure_port(f"""{self._listen_addr}:{self._listen_port}""")
        logging.info("Starting server on %s , port %s", self._listen_addr, self._listen_port)
        logging.info("File directory for this server located at %s", self._dir_for_files)
        await server.start()
        await server.wait_for_termination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--rest_ip", type=str, default="0.0.0.0:8000")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(ServerNode(args.port, args.rest_ip).serve())
