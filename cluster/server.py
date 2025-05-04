import election_pb2
import election_pb2_grpc
import heartbeat_pb2
import heartbeat_pb2_grpc
import transfer_file_pb2
import transfer_file_pb2_grpc

import argparse
import os
import grpc
import logging
import hashlib
import time
import asyncio
import requests

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
        print("Heartbeat from: ", request.ip_address, " pid:" ,request.pid, "storage in MB: ", request.storage)
        return heartbeat_pb2.HeartBeatResponse(ack=True)

class ElectionService(election_pb2_grpc.ElectionServiceServicer):
    def __init__(self, server_node):
        self.server_node = server_node

    def GetHash(self, request, context):
        return election_pb2.HashResponse(hash_value=self.server_node._hash_rank) 

class ServerNode:
    def __init__(self, port, rest_ip_addr):

        # Initialized Immutable Fields. These should not be changed, denoted by _ prefix.
        self._process_id = os.getpid()
        self._start_time = time.time()
        self._hash_rank = self._get_hash_rank()

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
        self.master_port: str = self._listen_port               
        self.master_rank = self._hash_rank

    def _get_hash_rank(self):
        combined = f"{self._process_id}-{self._start_time}".encode()
        return hashlib.sha256(combined).hexdigest()


    def print_dir_for_files(self) -> None:
        print(self._dir_for_files)

    async def get_registry(self):
        registry_request = requests.get(f"http://{self._rest_ip_addr}/registry")
        return registry_request.json()

    async def post_new_master(self, new_ip):
        body = {"ip": new_ip}
        x = requests.post(f"http://{self._rest_ip_addr}/master", json = body)
        print(x.text)
    
    # Election should block all other calls until a master is determined
    async def election(self):
        async with self._master_lock:
            registry = await self.get_registry()
            print(f"Registry for election {registry}")
            self.is_master = True
            self.master_addr = self._listen_addr
            self.master_port = self._listen_port               
            for node in registry:
                async with grpc.aio.insecure_channel(node) as channel:
                    stub = election_pb2_grpc.ElectionServiceStub(channel)
                    try:
                        print(f"Interfacing election with node {node}")
                        response = await stub.GetHash(election_pb2.HashRequest(hash_value=self._hash_rank), timeout=3)
                        print(f"Response hash_value: {response.hash_value}, self hash rank: {self._hash_rank}")
                        if (response.hash_value > self.master_rank):
                            addr_port = node.split(":")
                            self.master_addr = addr_port[0]
                            self.master_port = addr_port[1]
                            self.master_rank = response.hash_value
                            self.is_master = False            
                    except grpc.aio.AioRpcError as error:
                        if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                            print(f"Timeout on calling GetHash on node {node}, is it down?")
                        else:
                            print(f"Error on call {error}")
            
            if self.is_master:
                await self.post_new_master(self.master_addr+":"+self.master_port) 
        return

    async def heartBeat(self):
        while(True):
            async with self._master_lock:
                async with grpc.aio.insecure_channel(self.master_addr+":"+self.master_port) as channel:
                    stub = heartbeat_pb2_grpc.HeartBeatServiceStub(channel)
                    response = await stub.HeartBeat(heartbeat_pb2.HeartBeatRequest(pid=self._process_id, 
                                                                                   storage=0, 
                                                                                   ip_address=self._listen_addr+":"+self._listen_port), timeout=3) 
                    print(f"""Heart beating {self.master_addr}:{self.master_port} from {self._listen_addr}:{self._listen_port}. ACK = {response.ack}""")
            await asyncio.sleep(10)

    async def serve(self):
        server = grpc.aio.server()
        transfer_file_pb2_grpc.add_TransferFileServiceServicer_to_server(TransferFileService(self._dir_for_files), server)
        heartbeat_pb2_grpc.add_HeartBeatServiceServicer_to_server(HeartBeatService(), server)
        election_pb2_grpc.add_ElectionServiceServicer_to_server(ElectionService(self), server)
        server.add_insecure_port(f"""{self._listen_addr}:{self._listen_port}""")
        logging.info("Starting server on %s , port %s", self._listen_addr, self._listen_port)
        logging.info("File directory for this server located at %s", self._dir_for_files)
        await server.start()
        await asyncio.sleep(10)
        asyncio.create_task(self.election())
        asyncio.create_task(self.heartBeat())
        await server.wait_for_termination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=str, default="50051")
    parser.add_argument("--rest_ip", type=str, default="0.0.0.0:8000")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(ServerNode(args.port, args.rest_ip).serve())

