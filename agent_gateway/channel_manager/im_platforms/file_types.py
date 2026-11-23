# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

"""Shared file type detection for IM platform attachments."""

from __future__ import annotations


FILE_SIGNATURES = {
    # Images
    b'\x89PNG': '.png',
    b'\xff\xd8\xff': '.jpg',
    b'GIF8': '.gif',
    b'RIFF': '.webp',
    # Audio
    b'ID3': '.mp3',
    b'\xff\xfb': '.mp3',
    b'\xff\xfa': '.mp3',
    b'fLaC': '.flac',
    b'OggS': '.ogg',
    # Video
    b'ftyp': '.mp4',
    b'moof': '.mp4',
    b'moov': '.mp4',
    b'\x1a\x45\xdf\xa3': '.mkv',
    b'FLV': '.flv',
}


MIME_TYPES = {
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.webp': 'image/webp',
    '.mp3': 'audio/mpeg',
    '.wav': 'audio/wav',
    '.ogg': 'audio/ogg',
    '.flac': 'audio/flac',
    '.mp4': 'video/mp4',
    '.mkv': 'video/x-matroska',
    '.flv': 'video/x-flv',
    '.pdf': 'application/pdf',
    '.doc': 'application/msword',
    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.xls': 'application/vnd.ms-excel',
    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.ppt': 'application/vnd.ms-powerpoint',
    '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    '.txt': 'text/plain',
    '.zip': 'application/zip',
    '.json': 'application/json',
}


def detect_file_extension(content: bytes) -> str:
    """Detect a file extension from its magic bytes."""
    if len(content) < 12:
        return ''

    if content[:4] == b'RIFF' and content[8:12] == b'WEBP':
        return '.webp'

    if content[:4] == b'RIFF' and content[8:12] == b'WAVE':
        return '.wav'

    if content[4:8] in (b'ftyp', b'moof', b'moov'):
        return '.mp4'

    for signature, extension in FILE_SIGNATURES.items():
        if content.startswith(signature):
            return extension

    return ''


def get_mime_type(extension: str) -> str:
    """Return the MIME type for a file extension."""
    return MIME_TYPES.get(extension.lower(), 'application/octet-stream')
