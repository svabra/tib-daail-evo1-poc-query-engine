from __future__ import annotations

from importlib import import_module
from pkgutil import iter_modules

from .base import DataGenerator


IGNORED_MODULES = {"__init__", "base", "registry"}


class DataGeneratorRegistry:
    def discover(self) -> list[DataGenerator]:
        package = import_module(__package__)
        generators: list[DataGenerator] = []
        for module_info in iter_modules(package.__path__):
            if module_info.name in IGNORED_MODULES:
                continue
            module = import_module(f"{package.__name__}.{module_info.name}")
            generator = getattr(module, "GENERATOR", None)
            if isinstance(generator, DataGenerator):
                generators.append(generator)
        return sorted(generators, key=lambda item: item.title.lower())

    def definitions(self) -> list[dict[str, object]]:
        return [generator.definition().payload for generator in self.discover()]

    def generator(self, generator_id: str) -> DataGenerator:
        normalized = generator_id.strip()
        for generator in self.discover():
            if generator.generator_id == normalized:
                return generator
        raise KeyError(f"Unknown data generator: {generator_id}")
