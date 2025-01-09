import luigi
import wget
import os
import tarfile
import pandas as pd
import io

# Константы для удобства
DEFAULT_DATASET_NAME = "GSE68849"
DATA_DIR = "data"
COLUMNS_TO_DROP = [
	"Definition", "Ontology_Component", "Ontology_Process",
	"Ontology_Function", "Synonyms", "Obsolete_Probe_Id", "Probe_Sequence"
]


class DownloadDataset(luigi.Task):
	"""
	Задача для скачивания архива с данными.
	Использует параметр dataset_name для указания имени датасета.
	"""
	dataset_name = luigi.Parameter(default=DEFAULT_DATASET_NAME)

	def output(self):
		"""Указывает путь к скачанному архиву."""
		return luigi.LocalTarget(os.path.join(DATA_DIR, f"{self.dataset_name}_RAW.tar"))

	def run(self):
		"""Скачивает архив с данными."""
		os.makedirs(DATA_DIR, exist_ok=True)  # Создаем папку, если ее нет
		url = f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file"
		wget.download(url, out=self.output().path)


class ExtractArchive(luigi.Task):
	"""
	Задача для распаковки tar-архива.
	"""
	dataset_name = luigi.Parameter(default=DEFAULT_DATASET_NAME)

	def requires(self):
		"""Зависит от задачи DownloadDataset."""
		return DownloadDataset(dataset_name=self.dataset_name)

	def output(self):
		"""Указывает путь к папке с распакованными файлами."""
		return luigi.LocalTarget(os.path.join(DATA_DIR, f"{self.dataset_name}_extracted"))

	def run(self):
		"""Распаковывает tar-архив."""
		os.makedirs(self.output().path, exist_ok=True)
		with tarfile.open(self.input().path, "r") as tar:
			tar.extractall(path=self.output().path)


class SplitTables(luigi.Task):
	"""
	Задача для разделения текстовых файлов на отдельные tsv-таблицы.
	"""
	dataset_name = luigi.Parameter(default=DEFAULT_DATASET_NAME)

	def requires(self):
		"""Зависит от задачи ExtractArchive."""
		return ExtractArchive(dataset_name=self.dataset_name)

	def output(self):
		"""Указывает путь к папке с разделенными таблицами."""
		return luigi.LocalTarget(os.path.join(DATA_DIR, f"{self.dataset_name}_split"))

	def run(self):
		"""Разделяет текстовые файлы на tsv-таблицы."""
		os.makedirs(self.output().path, exist_ok=True)
		for filename in os.listdir(self.input().path):
			if filename.endswith(".txt"):
				self._split_file(os.path.join(self.input().path, filename))

	def _split_file(self, filepath):
		"""
		Вспомогательный метод для разделения одного файла на таблицы.
		"""
		dfs = {}
		write_key = None
		fio = io.StringIO()

		with open(filepath, "r") as f:
			for line in f:
				if line.startswith('['):
					if write_key:
						fio.seek(0)
						header = None if write_key == 'Heading' else 'infer'
						dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
					fio = io.StringIO()
					write_key = line.strip('[]\n')
					continue
				if write_key:
					fio.write(line)

			fio.seek(0)
			dfs[write_key] = pd.read_csv(fio, sep='\t')

		# Сохраняем таблицы в отдельные файлы
		for key, df in dfs.items():
			df.to_csv(os.path.join(self.output().path, f"{key}.tsv"), sep='\t', index=False)


class RemoveColumns(luigi.Task):
	"""
	Задача для удаления ненужных колонок из таблицы Probes.
	"""
	dataset_name = luigi.Parameter(default=DEFAULT_DATASET_NAME)

	def requires(self):
		"""Зависит от задачи SplitTables."""
		return SplitTables(dataset_name=self.dataset_name)

	def output(self):
		"""Указывает путь к папке с финальными данными."""
		return luigi.LocalTarget(os.path.join(DATA_DIR, f"{self.dataset_name}_final"))

	def run(self):
		"""Удаляет указанные колонки и сохраняет урезанную таблицу."""
		os.makedirs(self.output().path, exist_ok=True)
		probes_path = os.path.join(self.input().path, "Probes.tsv")
		df = pd.read_csv(probes_path, sep='\t')
		df.drop(columns=COLUMNS_TO_DROP, inplace=True)
		df.to_csv(os.path.join(self.output().path, "Probes_trimmed.tsv"), sep='\t', index=False)


class Cleanup(luigi.Task):
	"""
	Задача для удаления исходных текстовых файлов.
	"""
	dataset_name = luigi.Parameter(default=DEFAULT_DATASET_NAME)

	def requires(self):
		"""Зависит от задачи RemoveColumns."""
		return RemoveColumns(dataset_name=self.dataset_name)

	def output(self):
		"""Создает файл-маркер для указания завершения задачи."""
		return luigi.LocalTarget(os.path.join(self.input().path, "cleanup_complete.txt"))

	def run(self):
		"""Удаляет исходные текстовые файлы и создает файл-маркер."""
		for filename in os.listdir(self.input().path):
			if filename.endswith(".txt"):
				os.remove(os.path.join(self.input().path, filename))

		# Создаем файл-маркер
		with open(self.output().path, "w") as f:
			f.write("Cleanup completed successfully")


class Pipeline(luigi.WrapperTask):
	"""
	Финальная задача, объединяющая все шаги пайплайна.
	"""
	dataset_name = luigi.Parameter(default=DEFAULT_DATASET_NAME)

	def requires(self):
		"""Зависит от задачи Cleanup."""
		return Cleanup(dataset_name=self.dataset_name)