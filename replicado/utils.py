import unicodedata
from datetime import datetime
from typing import Any, Optional

def clean_string(value: Any) -> Any:
    """
    Remove espaços em branco do início e fim de strings.
    Útil para limpar retornos de colunas CHAR do Sybase.
    
    Args:
        value (Any): O valor a ser limpo.

    Returns:
        Any: A string limpa ou o valor original se não for string.
    """
    if isinstance(value, str):
        return value.strip()
    return value

def remove_accents(text: str) -> str:
    """
    Remove acentos de uma string.
    """
    return ''.join(c for c in unicodedata.normalize('NFD', text)
                  if unicodedata.category(c) != 'Mn')

def dia_semana(dia: str) -> str:
    """
    Converte código de dia da semana do replicado (ex: 2SG) para nome legível.
    """
    if not dia:
        return ''
        
    mapa = {
        '2SG': 'segunda-feira',
        '3TR': 'terça-feira',
        '4QA': 'quarta-feira',
        '5QI': 'quinta-feira',
        '6SX': 'sexta-feira',
        '7SB': 'sábado',
        '1DM': 'domingo',
    }
    return mapa.get(dia, '')

def horario_formatado(horario: str) -> str:
    """
    Formata horário (ex: 0830 -> 08:30).
    """
    if not horario:
        return horario
        
    s_horario = str(horario)
    if len(s_horario) == 4:
        return f"{s_horario[:2]}:{s_horario[2:]}"
    return s_horario

def data_mes(data: Any) -> Any:
    """
    Formata data para d/m/Y.
    Aceita string ISO ou objeto datetime.
    """
    if not data:
        return data
        
    if isinstance(data, datetime):
        return data.strftime('%d/%m/%Y')
        
    if isinstance(data, str):
        try:
            # Tenta converter string ISO (YYYY-MM-DD HH:MM:SS ou YYYY-MM-DD)
            # Simplificação: se tiver ' ', split.
            dt_part = data.split(' ')[0]
            dt = datetime.strptime(dt_part, '%Y-%m-%d')
            return dt.strftime('%d/%m/%Y')
        except ValueError:
            pass
            
    return data

import io
import zipfile
import xml.etree.ElementTree as ET

def unzip(zip_content: bytes) -> Optional[bytes]:
    """
    Descompacta o primeiro arquivo de um conteúdo binário ZIP.
    """
    if not zip_content:
        return None
        
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            if not zf.namelist():
                return None
            return zf.read(zf.namelist()[0])
    except Exception:
        return None

    except Exception:
        return None

def etree_to_dict(t) -> Dict[str, Any]:
    """
    Converte um ElementTree para dicionário, estrutura similar ao json_encode(simplexml) do PHP.
    Atributos ficam em '@attributes'.
    """
    d = {}
    if t.attrib:
        d['@attributes'] = dict(t.attrib)
        
    children = list(t)
    if children:
        dd = {}
        for child in children:
            child_d = etree_to_dict(child)
            # child_d is {tag: content}
            for k, v in child_d.items():
                if k in dd:
                    if isinstance(dd[k], list):
                        dd[k].append(v)
                    else:
                        dd[k] = [dd[k], v]
                else:
                    dd[k] = v
        d.update(dd)
    
    if t.text:
        text = t.text.strip()
        if text:
            if children or t.attrib:
                 pass # Ignora texto misto por enquanto, foca em dados estruturados
            else:
                 return {t.tag: text}
    
    return {t.tag: d} if d or (t.attrib or children) else {t.tag: ''}

def get_path(data: Dict[str, Any], path: str, default: Any = None) -> Any:
    """
    Emula Arr::get do Laravel (dot notation).
    """
    keys = path.split('.')
    val = data
    for key in keys:
        if isinstance(val, dict) and key in val:
            val = val[key]
        else:
            return default
    return val
