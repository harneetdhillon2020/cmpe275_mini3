import rest_to_main_pb2
import rest_to_main_pb2_grpc
import grpc
import logging
import asyncio

DIR_FOR_FILES = "~/Desktop/uploads"

class TransferFileServer(rest_to_main_pb2_grpc.TransferFileServiceServicer):

    def transferFile(self, request_iterator, context):
        stream_iterator = iter(request_iterator) 
        try:
            stream_data = next(stream_iterator)
        except StopIteration:
            return rest_to_main_pb2.transferResponse(success=False, status_message="Could not parse stream")
        
        # First chunk receive should be 0, then we make the filepath off of the filename
        current_chunk = 0
        if stream_data.chunk_number == current_chunk:
            filename = stream_data.filename
            filepath = f"DIR_FOR_FILES/{filename}"
        else:
            return rest_to_main_pb2.transferResponse(success=False, status_message="Chunk number not matching")

        # Open up a file in the OS and write, each time we write, we should update the chunk number to the next and check if its matching. Hope it works
        with open(filepath, "wb") as file_pointer:
            file_pointer.write(stream_data.data)
            current_chunk += 1
            for remaining_chunk in stream_iterator:
                if (current_chunk != remaining_chunk.chunk_number):
                    return rest_to_main_pb2.transferResponse(success=False, status_message="Chunk number not matching")
                file_pointer.write(remaining_chunk.data)
                current_chunk += 1
        return rest_to_main_pb2.transferResponse(success=True, status_message="Stream parse finished")


async def serve() -> None:
    server = grpc.aio.server()
    rest_to_main_pb2_grpc.add_TransferFileServiceServicer_to_server(TransferFileServer(), server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())



