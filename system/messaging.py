from __future__ import annotations

from system.types import Event


def split_message(s: str, *, maxlen: int = 380) -> list[str]:
    s = (s or "").strip()
    if not s:
        return []
    if maxlen <= 0 or len(s) <= maxlen:
        return [s]
    parts = s.split(" ")
    out: list[str] = []
    cur = ""
    for p in parts:
        if not p:
            continue
        if not cur:
            cur = p
            continue
        if len(cur) + 1 + len(p) > maxlen:
            out.append(cur)
            cur = p
        else:
            cur += " " + p
    if cur:
        out.append(cur)
    return out


async def send_user_message(
    bot,
    ev: Event,
    text: str,
    *,
    scope: str = "same",
    mention: bool = True,
    split: bool = True,
    maxlen: int = 380,
    private: bool = False,
    allow_empty: bool = False,
) -> None:
    msg = "" if text is None else str(text).strip()
    if not msg and not allow_empty:
        return

    mode = "pm" if private else (scope or "same").strip().lower()
    if mode not in {"same", "pm", "channel"}:
        mode = "same"

    if mode == "pm":
        target = ev.nick
    elif mode == "channel":
        target = ev.channel or ev.target
    else:
        target = ev.target

    if mention and ev.nick:
        msg = f"{ev.nick}: {msg}"

    lines = split_message(msg, maxlen=maxlen) if split else [msg]
    for line in lines:
        if line or allow_empty:
            await bot.privmsg(target, line)
