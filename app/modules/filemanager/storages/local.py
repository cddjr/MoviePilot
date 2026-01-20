import asyncio
import aioshutil
from aiopathlib import AsyncPath
from aiofiles import open
from pathlib import Path
from typing import Optional, List

from app import schemas
from app.core.config import global_vars
from app.helper.directory import DirectoryHelper
from app.log import logger
from app.modules.filemanager.storages import AsyncStorageBase, transfer_process
from app.schemas.types import StorageSchema
from app.utils.common import forward_to_async
from app.utils.system import SystemUtils


class LocalStorage(AsyncStorageBase):
    """
    本地文件操作
    """

    # 存储类型
    schema = StorageSchema.Local
    # 支持的整理方式
    transtype = {
        "copy": "复制",
        "move": "移动",
        "link": "硬链接",
        "softlink": "软链接"
    }

    # 文件块大小，默认10MB
    chunk_size = 10 * 1024 * 1024

    def init_storage(self):
        """
        初始化
        """
        pass

    def check(self) -> bool:
        """
        检查存储是否可用
        """
        return True

    async def __get_fileitem(self, path: AsyncPath) -> schemas.FileItem:
        """
        获取文件项
        """
        stat_info = await path.stat()
        return schemas.FileItem(
            storage=self.schema.value,
            type="file",
            path=path.as_posix(),
            name=path.name,
            basename=path.stem,
            extension=path.suffix[1:],
            size=stat_info.st_size,
            modify_time=stat_info.st_mtime,
        )

    async def __get_diritem(self, path: AsyncPath) -> schemas.FileItem:
        """
        获取目录项
        """
        stat_info = await path.stat()
        return schemas.FileItem(
            storage=self.schema.value,
            type="dir",
            path=path.as_posix() + "/",
            name=path.name,
            basename=path.stem,
            modify_time=stat_info.st_mtime,
        )

    async def async_list(self, fileitem: schemas.FileItem) -> List[schemas.FileItem]:
        """
        浏览文件
        """
        # 返回结果
        ret_items = []
        path = fileitem.path
        if not fileitem.path or fileitem.path == "/":
            if SystemUtils.is_windows():
                partitions = SystemUtils.get_windows_drives() or ["C:/"]
                for partition in partitions:
                    ret_items.append(schemas.FileItem(
                        storage=self.schema.value,
                        type="dir",
                        path=partition + "/",
                        name=partition,
                        basename=partition
                    ))
                return ret_items
            else:
                path = "/"
        else:
            if SystemUtils.is_windows():
                path = path.lstrip("/")
            elif not path.startswith("/"):
                path = "/" + path

        # 遍历目录
        path_obj = AsyncPath(path)
        if not await path_obj.exists():
            logger.warn(f"【本地】目录不存在：{path}")
            return []

        # 如果是文件
        if await path_obj.is_file():
            ret_items.append(await self.__get_fileitem(path_obj))
            return ret_items

        async def __process_items(items, get_item_func):
            """并发处理一组项，返回处理结果列表"""
            tasks = [get_item_func(item) for item in items]
            return await asyncio.gather(*tasks)

        # 获取目录和文件列表
        directory_items = await SystemUtils.async_list_sub_directory(path_obj)
        file_items = await SystemUtils.async_list_sub_file(path_obj)

        # 分别并发处理目录和文件
        dir_results = await __process_items(directory_items, self.__get_diritem)
        file_results = await __process_items(file_items, self.__get_fileitem)

        # 合并结果
        ret_items = dir_results + file_results
        return ret_items

    @forward_to_async(target=async_list)
    def list(self, fileitem: schemas.FileItem) -> List[schemas.FileItem]:
        pass

    async def async_create_folder(self, fileitem: schemas.FileItem, name: str) -> Optional[schemas.FileItem]:
        """
        创建目录
        :param fileitem: 父目录
        :param name: 目录名
        """
        if not fileitem.path:
            return None
        path_obj = AsyncPath(fileitem.path) / name
        if not await path_obj.exists():
            await  path_obj.mkdir(parents=True)
        return await self.__get_diritem(path_obj)

    @forward_to_async(target=async_create_folder)
    def create_folder(self, fileitem: schemas.FileItem, name: str) -> Optional[schemas.FileItem]:
        pass

    async def async_get_folder(self, path: AsyncPath) -> Optional[schemas.FileItem]:
        """
        获取目录
        """
        if not await path.exists():
            await path.mkdir(parents=True, exist_ok=True)
        return await self.__get_diritem(path)

    @forward_to_async(target=async_get_folder)
    def get_folder(self, path: Path) -> Optional[schemas.FileItem]:
        pass

    async def async_get_item(self, path: AsyncPath) -> Optional[schemas.FileItem]:
        """
        获取文件或目录，不存在返回None
        """
        if not await path.exists():
            return None
        if await path.is_file():
            return await self.__get_fileitem(path)
        return await self.__get_diritem(path)

    @forward_to_async(target=async_get_item)
    def get_item(self, path: Path) -> Optional[schemas.FileItem]:
        pass

    async def async_detail(self, fileitem: schemas.FileItem) -> Optional[schemas.FileItem]:
        """
        获取文件详情
        """
        path_obj = AsyncPath(fileitem.path)
        if not await path_obj.exists():
            return None
        return await self.__get_fileitem(path_obj)

    @forward_to_async(target=async_detail)
    def detail(self, fileitem: schemas.FileItem) -> Optional[schemas.FileItem]:
        pass

    async def async_delete(self, fileitem: schemas.FileItem) -> bool:
        """
        删除文件
        """
        if not fileitem.path:
            return False
        path_obj = AsyncPath(fileitem.path)
        if not await path_obj.exists():
            return True
        try:
            if await path_obj.is_file():
                await  path_obj.unlink()
            else:
                await aioshutil.rmtree(path_obj, ignore_errors=True)
        except Exception as e:
            logger.error(f"【本地】删除文件失败：{e}")
            return False
        return True

    @forward_to_async(target=async_delete)
    def delete(self, fileitem: schemas.FileItem) -> bool:
        pass

    async def async_rename(self, fileitem: schemas.FileItem, name: str) -> bool:
        """
        重命名文件
        """
        path_obj = AsyncPath(fileitem.path)
        if not await path_obj.exists():
            return False
        try:
            await path_obj.rename(path_obj.parent / name)
        except Exception as e:
            logger.error(f"【本地】重命名文件失败：{e}")
            return False
        return True

    @forward_to_async(target=async_rename)
    def rename(self, fileitem: schemas.FileItem, name: str) -> bool:
        pass

    async def async_download(self, fileitem: schemas.FileItem, path: AsyncPath = None) -> Optional[AsyncPath]:
        """
        下载文件
        """
        return AsyncPath(fileitem.path)

    @forward_to_async(target=async_download)
    def download(self, fileitem: schemas.FileItem, path: Path = None) -> Optional[Path]:
        pass

    async def _copy_with_progress(self, src: AsyncPath, dest: AsyncPath):
        """
        分块复制文件并回调进度
        """
        total_size = (await src.stat()).st_size
        copied_size = 0
        progress_callback = transfer_process(src.as_posix())
        try:
            async with open(src, "rb") as fsrc, open(dest, "wb") as fdst:
                while True:
                    if global_vars.is_transfer_stopped(src.as_posix()):
                        logger.info(f"【本地】{src} 复制已取消！")
                        return False
                    buf = await fsrc.read(self.chunk_size)
                    if not buf:
                        break
                    await fdst.write(buf)
                    copied_size += len(buf)
                    # 更新进度
                    if progress_callback:
                        percent = copied_size / total_size * 100
                        progress_callback(percent)
            # 保留文件时间戳、权限等信息
            await aioshutil.copystat(src, dest)
            return True
        except Exception as e:
            logger.error(f"【本地】复制文件 {src} 失败：{e}")
            return False
        finally:
            progress_callback(100)

    async def async_upload(
            self,
            fileitem: schemas.FileItem,
            path: AsyncPath,
            new_name: Optional[str] = None
    ) -> Optional[schemas.FileItem]:
        """
        上传文件（带进度）
        """
        try:
            dir_path = AsyncPath(fileitem.path)
            target_path = dir_path / (new_name or path.name)
            if await self._copy_with_progress(path, target_path):
                # 上传删除源文件
                await path.unlink()
                return await self.async_get_item(target_path)
        except Exception as err:
            logger.error(f"【本地】移动文件失败：{err}")
        return None

    @forward_to_async(target=async_upload)
    def upload(
            self,
            fileitem: schemas.FileItem,
            path: Path,
            new_name: Optional[str] = None
    ) -> Optional[schemas.FileItem]:
        pass

    @staticmethod
    def __should_show_progress(src: Path, dest: Path):
        """
        是否显示进度条
        """
        src_isnetwork = SystemUtils.is_network_filesystem(src)
        dest_isnetwork = SystemUtils.is_network_filesystem(dest)
        if src_isnetwork and dest_isnetwork and SystemUtils.is_same_disk(src, dest):
            return True
        return False

    async def async_copy(
            self,
            fileitem: schemas.FileItem,
            path: AsyncPath,
            new_name: str
    ) -> bool:
        """
        复制文件（带进度）
        """
        try:
            src = AsyncPath(fileitem.path)
            dest = path / new_name
            if self.__should_show_progress(src, dest):
                if await self._copy_with_progress(src, dest):
                    return True
            else:
                code, message = await SystemUtils.async_copy(src, dest)
                if code == 0:
                    return True
                else:
                    logger.error(f"【本地】复制文件失败：{message}")
        except Exception as err:
            logger.error(f"【本地】复制文件失败：{err}")
        return False

    @forward_to_async(target=async_copy)
    def copy(
            self,
            fileitem: schemas.FileItem,
            path: Path,
            new_name: str
    ) -> bool:
        pass

    async def async_move(
            self,
            fileitem: schemas.FileItem,
            path: AsyncPath,
            new_name: str
    ) -> bool:
        """
        移动文件（带进度）
        """
        try:
            src = AsyncPath(fileitem.path)
            dest = path / new_name
            if src == dest:
                # 目标和源文件相同，直接返回成功，不做任何操作
                return True
            if self.__should_show_progress(src, dest):
                if await self._copy_with_progress(src, dest):
                    # 复制成功删除源文件
                    await src.unlink()
                    return True
            else:
                code, message = await SystemUtils.async_move(src, dest)
                if code == 0:
                    return True
                else:
                    logger.error(f"【本地】移动文件失败：{message}")
        except Exception as err:
            logger.error(f"【本地】移动文件失败：{err}")
        return False

    @forward_to_async(target=async_move)
    def move(
            self,
            fileitem: schemas.FileItem,
            path: Path,
            new_name: str
    ) -> bool:
        pass

    async def async_link(self, fileitem: schemas.FileItem, target_file: AsyncPath) -> bool:
        """
        硬链接文件
        """
        file_path = AsyncPath(fileitem.path)
        code, message = await SystemUtils.async_link(file_path, target_file)
        if code != 0:
            logger.error(f"【本地】硬链接文件失败：{message}")
            return False
        return True

    @forward_to_async(target=async_link)
    def link(self, fileitem: schemas.FileItem, target_file: Path) -> bool:
        pass

    async def async_softlink(self, fileitem: schemas.FileItem, target_file: AsyncPath) -> bool:
        """
        软链接文件
        """
        file_path = AsyncPath(fileitem.path)
        code, message = await SystemUtils.async_softlink(file_path, target_file)
        if code != 0:
            logger.error(f"【本地】软链接文件失败：{message}")
            return False
        return True

    @forward_to_async(target=async_softlink)
    def softlink(self, fileitem: schemas.FileItem, target_file: Path) -> bool:
        pass

    async def async_usage(self) -> Optional[schemas.StorageUsage]:
        """
        存储使用情况
        """
        directory_helper = DirectoryHelper()
        total_storage, free_storage = SystemUtils.space_usage(
            [AsyncPath(d.download_path) for d in directory_helper.get_local_download_dirs() if d.download_path] +
            [AsyncPath(d.library_path) for d in directory_helper.get_local_library_dirs() if d.library_path]
        )
        return schemas.StorageUsage(
            total=total_storage,
            available=free_storage
        )

    @forward_to_async(target=async_usage)
    def usage(self) -> Optional[schemas.StorageUsage]:
        pass
