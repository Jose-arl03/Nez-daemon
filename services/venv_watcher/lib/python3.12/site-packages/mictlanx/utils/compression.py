import zlib
import gzip
from enum import Enum,auto
from typing import Dict,Any,Generator
import humanfriendly as HF
import io 
from option import Result, Ok,Err
try:
    import lz4.frame
    LZ4_AVAILABLE = True
except ImportError:
    LZ4_AVAILABLE = False


class CompressionAlgorithm(Enum):
    ZLIB = auto()
    GZIP = auto()
    LZ4  = auto()


class CompressionX:
    """Utility class for compressing and decompressing data using zlib, gzip, and lz4."""

    # -------------------- ZLIB Compression --------------------
    @staticmethod
    def compress_zlib_stream(data: bytes, level: int = 9) -> bytes:
        """Compresses bytes using zlib."""
        compressor = zlib.compressobj(level)
        compressed = compressor.compress(data) + compressor.flush()
        return compressed

    @staticmethod
    def decompress_zlib_stream(compressed_data: bytes, chunk_size: int = 1024):
        """Generator to decompress zlib-compressed data in chunks."""
        decompressor = zlib.decompressobj()
        for i in range(0, len(compressed_data), chunk_size):
            yield decompressor.decompress(compressed_data[i:i + chunk_size])
        yield decompressor.flush()

    @staticmethod
    def compress_zlib(input_path: str, output_path: str, level: int = 9):
        """Compresses a file using zlib and saves the result."""
        with open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
            f_out.write(CompressionX.compress_zlib_stream(f_in.read(), level))

    @staticmethod
    def decompress_zlib(input_path: str, output_path: str, chunk_size: int = 1024):
        """Decompresses a zlib-compressed file and writes output to a new file."""
        with open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
            for chunk in CompressionX.decompress_zlib_stream(f_in.read(), chunk_size):
                f_out.write(chunk)

    # -------------------- GZIP Compression --------------------
    @staticmethod
    def compress_gzip_stream(data: bytes,level:int = 9) -> bytes:
        """Compresses bytes using gzip."""
        return gzip.compress(data,compresslevel=level)

    @staticmethod
    def decompress_gzip_stream(compressed_data: bytes, chunk_size: int = 1024):
        """Generator to decompress gzip-compressed data in chunks."""
        with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as f:
            while chunk := f.read(chunk_size):
                yield chunk

    @staticmethod
    def compress_gzip(input_path: str, output_path: str):
        """Compresses a file using gzip."""
        with open(input_path, "rb") as f_in, gzip.open(output_path, "wb") as f_out:
            f_out.writelines(f_in)

    @staticmethod
    def decompress_gzip(input_path: str, output_path: str, chunk_size: int = 1024):
        """Decompresses a gzip-compressed file and writes output to a new file."""
        with gzip.open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
            while chunk := f_in.read(chunk_size):
                f_out.write(chunk)

    # -------------------- LZ4 Compression (Optional) --------------------
    @staticmethod
    def compress_lz4_stream(data: bytes, compression_level: int = 9,chunk_size:str="1MB") -> bytes:
    # Generator[bytes,None,None]:
        """Compress data using LZ4."""
        if not LZ4_AVAILABLE:
            raise ImportError("lz4 is not installed. Install with `pip install lz4`")
        return lz4.frame.compress(data, compression_level=compression_level)

    @staticmethod
    def decompress_lz4_stream(compressed_data: bytes,chunk_size:int = 1024):
        """Generator to stream decompress LZ4-compressed data in chunks."""
        if not LZ4_AVAILABLE:
            raise ImportError("lz4 is not installed. Install with `pip install lz4`")
        # decompressor = lz4.frame.LZ4FrameDecompressor()
        return lz4.frame.decompress(compressed_data)
    @staticmethod
    def decompress_lz4_stream_gen(compressed_data: bytes, chunk_size: int = 1024):
        """Generator to stream decompress LZ4-compressed data in chunks."""
        if not LZ4_AVAILABLE:
            raise ImportError("lz4 is not installed. Install with `pip install lz4`")
        decompressor = lz4.frame.LZ4FrameDecompressor()
        i = 0
        while i < len(compressed_data):
            chunk = decompressor.decompress(compressed_data[i:i + chunk_size])
            if chunk:
                yield chunk  # ✅ Yield only when there's data
            i += chunk_size

    @staticmethod
    def compress_lz4(input_path: str, output_path: str, compression_level: int = 9):
        """Compresses a file using LZ4."""
        if not LZ4_AVAILABLE:
            raise ImportError("lz4 is not installed. Install with `pip install lz4`")
        with open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
            f_out.write(CompressionX.compress_lz4_stream(f_in.read(), compression_level))

    @staticmethod
    def decompress_lz4(input_path: str, output_path: str, chunk_size: int = 1024):
        """Decompresses an LZ4-compressed file and writes output to a new file."""
        if not LZ4_AVAILABLE:
            raise ImportError("lz4 is not installed. Install with `pip install lz4`")
        with open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
            for chunk in CompressionX.decompress_lz4_stream(f_in.read(), chunk_size):
                f_out.write(chunk)
    
    @staticmethod
    def compress_stream(
        algorithm:CompressionAlgorithm,
        data:bytes,
        params:Dict[str, Any]= {}
    )->Result[bytes, Exception]:
        try: 
            if algorithm == CompressionAlgorithm.ZLIB:
                return Ok(CompressionX.compress_zlib_stream(data= data, **params))
            elif algorithm == CompressionAlgorithm.GZIP: 
                return Ok(CompressionX.compress_gzip_stream(data=data, **params))
            elif algorithm == CompressionAlgorithm.LZ4:
                return Ok(CompressionX.compress_lz4_stream(data=data, **params))
        except Exception as e:
            return Err(e)
    @staticmethod
    def decompress_stream_gen(
        algorithm:CompressionAlgorithm,
        data:bytes,
        chunk_size:str = "1MB"
    )->Result[Generator[bytes,None,None],Exception]:
        try:
            if algorithm == CompressionAlgorithm.ZLIB:
                return Ok(CompressionX.decompress_zlib_stream(compressed_data= data, chunk_size=HF.parse_size(chunk_size)))
            elif algorithm == CompressionAlgorithm.GZIP: 
                return Ok(CompressionX.decompress_gzip_stream(compressed_data=data, chunk_size=HF.parse_size(chunk_size) ))
            elif algorithm == CompressionAlgorithm.LZ4:
                return Ok(CompressionX.decompress_lz4_stream(compressed_data=data, chunk_size=HF.parse_size(chunk_size) ))
        except Exception as e:
            return Err(e)
    @staticmethod
    def decompress_stream(
        algorithm:CompressionAlgorithm,
        data:bytes,
        chunk_size:str = "1MB"
    )->Result[bytes,Exception]:
        try:
            buffer = io.BytesIO()

            if algorithm == CompressionAlgorithm.ZLIB:
                gen = CompressionX.decompress_zlib_stream(compressed_data= data, chunk_size=HF.parse_size(chunk_size))
            elif algorithm == CompressionAlgorithm.GZIP: 
                gen = CompressionX.decompress_gzip_stream(compressed_data=data, chunk_size=HF.parse_size(chunk_size) )
            elif algorithm == CompressionAlgorithm.LZ4:
                gen = CompressionX.decompress_lz4_stream(compressed_data=data, chunk_size=HF.parse_size(chunk_size) )

            for chunk in gen:
                buffer.write(chunk)
            val = buffer.getvalue()
            buffer.close()
            return Ok(val)
        
        except Exception as e:
            return Err(e)
