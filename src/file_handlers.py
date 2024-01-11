from abc import ABC, abstractmethod
from io import StringIO, BytesIO
import zipfile
import re
from collections.abc import Generator

import chardet
import fsspec


class FileHandler(ABC):
    """Abstract FileHandler class that all file handlers should inherit from."""

    @abstractmethod
    def open(self, file_path: str) -> Generator[StringIO | BytesIO, None, None]:
        """Open a file and return a file like object.

        Args:
            file_path (str): The path to the file to open.

        Yields:
            Generator[StringIO | BytesIO, None, None]: A file like object.
        """


class BasicFileHandler(FileHandler):
    """Basic file handler that reads the entire file into memory."""

    def __init__(self, encoding: str = "utf8"):
        self.encoding = encoding

    def open(self, file_path: str) -> Generator[StringIO, None, None]:
        with fsspec.open(file_path) as f:
            string = f.read().decode(self.encoding)
        yield StringIO(string)


class ChunkedFileHandler(FileHandler):
    """Chunked file handler that reads the file in chunks."""

    def __init__(self, chunk_size: int = 1024 * 1024 * 50, encoding: str = "utf8"):
        self.encoding = encoding
        self.chunk_size = chunk_size

    def open(self, file_path: str) -> Generator[StringIO, None, None]:
        with fsspec.open(file_path) as f:
            while True:
                new_content = f.read(self.chunk_size)
                new_content += f.readline()
                if not new_content:
                    return
                yield StringIO(new_content.decode(self.encoding))


class SeparatedFileHandler(FileHandler):
    """Separated file handler that splits the file on a separator and yields each chunk."""

    def __init__(
        self,
        separator: str = "{s}CHUNK{s}\n".format(s="#" * 114),
        encoding: str = "utf8",
    ):
        self.encoding = encoding
        self.separator = separator

    def open(self, file_path: str) -> Generator[StringIO, None, None]:
        with fsspec.open(file_path) as f:
            string = f.read().decode(self.encoding)
            for i in string.split(self.separator):
                if i:
                    yield StringIO(i.strip())


class ZipFileHandler(FileHandler):
    """Zip file handler that finds files matching a regex and yields the contents of each file."""

    def __init__(
        self, regex: str = r".*", encoding: str = "utf8", is_binary_file: bool = False
    ):
        self.encoding = encoding
        self.regex = regex
        self.is_binary_file = is_binary_file

    def open(self, file_path: str) -> Generator[StringIO | BytesIO, None, None]:
        zip_file = fsspec.open(file_path)
        with zipfile.ZipFile(zip_file, "r") as zf:
            matching_files = [
                f for f in zf.namelist() if re.match(self.regex, f) is not None
            ]

            for filename in matching_files:
                bytes = zf.read(filename)
                if self.is_binary_file:
                    file_obj = BytesIO(bytes)
                else:
                    uni_string = self._to_unicode(bytes)
                    file_obj = StringIO(uni_string)
                file_obj.name = filename
                yield file_obj
        return

    def _to_unicode(self, raw: bytes) -> str:
        """Try to decode the bytes using the specified encoding. If that fails, try to infer the encoding.

        Args:
            raw (bytes): The bytes to decode.

        Raises:
            ValueError: If the encoding cannot be inferred.

        Returns:
            str: The decoded string.

        """
        try:
            return str(raw, self.encoding)
        except UnicodeDecodeError:
            encoding = chardet.detect(raw)["encoding"]
            if encoding is None:
                raise ValueError("Could not infer encoding")
            else:
                return str(raw, encoding)


class BinaryFileHandler(FileHandler):
    """Binary file handler that reads the entire file into memory."""

    def __init__(self, encoding: str = "utf8"):
        self.encoding = encoding

    def open(self, file_path: str) -> Generator[BytesIO, None, None]:
        with fsspec.open(file_path, "rb") as f:
            string = f.read()
            yield BytesIO(string)
        return
