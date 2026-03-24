import shutil
import os

def file_delete(folder_path):

    try:
        # 获取文件夹大小
        total_size = 0
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    total_size += os.path.getsize(file_path)
                except:
                    pass

        # 遍历文件夹
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)

            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)

                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)


            except Exception as e:
                print(e)

    except Exception as e:
        print(e)


if __name__ == "__main__":
    file_delete(r'C:\Users\Administrator\Downloads')
