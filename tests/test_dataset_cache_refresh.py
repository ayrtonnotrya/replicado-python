"""Regressão do refresh incremental de cache em ``carregar_dados``.

Exercita as 4 combinações do contrato consumido pela API Skuld, sem bater no
banco (mocka extração/carga). Garante que:

- ``forcar=False, atualizar_anos=()``  -> só lê pickles de HIST (backward-compat).
- ``forcar=True,  atualizar_anos=()``  -> re-extraí TODAS as fatias de HIST
  (bug fix: ``forcar`` agora honra o nome no histórico).
- ``forcar=False, atualizar_anos=[2024]`` -> re-extraí SÓ 2024; 2023 do cache
  (refresh cirúrgico / lean).
- ``forcar=True,  atualizar_anos=[2024]`` -> re-extraí SÓ 2024 (``atualizar_anos``
  SOBREESCREVE ``forcar`` para a HISTESCOLARGR); 2023 do cache. É o caso da
  chamada única de retreino da Skuld.
"""

from __future__ import annotations

import pandas as pd
import pytest

import replicado.cache as cache_mod
import replicado.dataset_alocacao as dsa
import replicado.dataset_macrosensores as macros_mod
from replicado.dataset_alocacao import DatasetConfig, carregar_dados

HIST_COLS = ["coddis", "codtur", "dtacrihst", "dtaultalt"]
TURMAS_COLS = ["coddis", "codtur"]


def _empty_hist_df() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype=object) for c in HIST_COLS})


def _empty_turmas_df() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype=object) for c in TURMAS_COLS})


def _make_cfg(tmp_path) -> DatasetConfig:
    return DatasetConfig(
        codundclg=45,
        prefixos=("MAC",),
        ano_min=2023,
        ano_max=2024,
        cache_dir=tmp_path,
    )


def _seed_hist_pickles(tmp_path, anos=(2023, 2024)) -> None:
    for ano in anos:
        _empty_hist_df().to_pickle(tmp_path / f"histescolar_{ano}.pkl")
    _empty_turmas_df().to_pickle(tmp_path / "turmagr_full.pkl")


def _patch_loaders(monkeypatch, extracted, loaded):
    # Lazy import dentro do loop: patcha cache_mod.extrair_fatia_histescolar.
    def _fake_extract(ano, cache_dir=None):
        extracted.append(ano)
        return _empty_hist_df()

    monkeypatch.setattr(cache_mod, "extrair_fatia_histescolar", _fake_extract)

    # _load_pickled é global do dataset_alocacao; registra o NOME do pickle.
    def _fake_load(caminho):
        loaded.append(caminho.name)
        # turmagr_full.pkl precisa de coddis/codtur; HIST precisa das 4 colunas.
        if caminho.name.startswith("histescolar_"):
            return _empty_hist_df()
        return _empty_turmas_df()

    monkeypatch.setattr(dsa, "_load_pickled", _fake_load)

    # TURMAGR e auxiliares via _stream_to_pickle (quando forcar/ausente).
    def _fake_stream(caminho, query, desc, chunksize=5000):
        return _empty_turmas_df()

    monkeypatch.setattr(dsa, "_stream_to_pickle", _fake_stream)

    # Macro-sensores: lazy import; no-op.
    monkeypatch.setattr(macros_mod, "carregar_macrosensores", lambda *a, **k: None)


def _hist_loaded(loaded):
    return [n for n in loaded if n.startswith("histescolar_")]


def test_atualizar_vazio_forcar_false_le_do_cache(tmp_path, monkeypatch):
    _seed_hist_pickles(tmp_path)
    extracted: list[int] = []
    loaded: list[str] = []
    _patch_loaders(monkeypatch, extracted, loaded)
    carregar_dados(_make_cfg(tmp_path), forcar=False, atualizar_anos=())
    assert extracted == []
    assert sorted(_hist_loaded(loaded)) == ["histescolar_2023.pkl", "histescolar_2024.pkl"]


def test_atualizar_vazio_forcar_true_reextrai_tudo(tmp_path, monkeypatch):
    _seed_hist_pickles(tmp_path)
    extracted: list[int] = []
    loaded: list[str] = []
    _patch_loaders(monkeypatch, extracted, loaded)
    carregar_dados(_make_cfg(tmp_path), forcar=True, atualizar_anos=())
    assert sorted(extracted) == [2023, 2024]
    assert _hist_loaded(loaded) == []


def test_atualizar_seletivo_forcar_false(tmp_path, monkeypatch):
    _seed_hist_pickles(tmp_path)
    extracted: list[int] = []
    loaded: list[str] = []
    _patch_loaders(monkeypatch, extracted, loaded)
    carregar_dados(_make_cfg(tmp_path), forcar=False, atualizar_anos=[2024])
    assert extracted == [2024]
    assert _hist_loaded(loaded) == ["histescolar_2023.pkl"]


def test_atualizar_seletivo_sobrescreve_forcar(tmp_path, monkeypatch):
    """Caso do retreino Skuld: forcar=True + atualizar_anos=[2024] -> só 2024
    é re-extraído (atualizar_anos governa a HIST; forcar é ignorado p/ HIST)."""
    _seed_hist_pickles(tmp_path)
    extracted: list[int] = []
    loaded: list[str] = []
    _patch_loaders(monkeypatch, extracted, loaded)
    carregar_dados(_make_cfg(tmp_path), forcar=True, atualizar_anos=[2024])
    assert extracted == [2024]
    assert _hist_loaded(loaded) == ["histescolar_2023.pkl"]


def test_montar_dataset_acepta_atualizar_anos(monkeypatch):
    """A assinatura de montar_dataset expõe atualizar_anos (keyword-only) e
    repassa a carregar_dados."""
    import inspect

    sig = inspect.signature(dsa.montar_dataset)
    assert "atualizar_anos" in sig.parameters
    assert sig.parameters["atualizar_anos"].kind == inspect.Parameter.KEYWORD_ONLY
    assert sig.parameters["atualizar_anos"].default == ()
    # carregar_dados também keyword-only.
    sig2 = inspect.signature(dsa.carregar_dados)
    assert sig2.parameters["atualizar_anos"].kind == inspect.Parameter.KEYWORD_ONLY


def test_cache_module_exposes_extratores(monkeypatch):
    from replicado.cache import (
        COLS_HIST,
        DEFAULT_CACHE_DIR,
        extrair_fatia_histescolar,
        extrair_histescolar,
        extrair_turmagr,
    )

    assert COLS_HIST.strip() and "codpes" in COLS_HIST
    assert DEFAULT_CACHE_DIR.name == "cache_maquina_tempo"
    for fn in (extrair_turmagr, extrair_fatia_histescolar, extrair_histescolar):
        assert callable(fn)


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
