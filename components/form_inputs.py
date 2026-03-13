import streamlit as st


def clearable_text_input(
    label: str,
    key: str,
    placeholder: str = "",
    value: str = "",
    container=None,
    refresh_on_value_change: bool = False,
    **kwargs,
) -> str:
    """× ボタン付き text_input。セッションステートで値を管理する。"""
    ss_key = f"_cti_{key}"
    if ss_key not in st.session_state:
        st.session_state[ss_key] = value
    elif refresh_on_value_change and value != st.session_state.get(ss_key, ""):
        st.session_state[ss_key] = value
        st.session_state.pop(key, None)

    inp_col, btn_col = (container or st).columns([10, 1])
    if key in st.session_state:
        val = inp_col.text_input(
            label,
            placeholder=placeholder,
            key=key,
            **kwargs,
        )
    else:
        val = inp_col.text_input(
            label,
            value=st.session_state[ss_key],
            placeholder=placeholder,
            key=key,
            **kwargs,
        )
    st.session_state[ss_key] = val
    if btn_col.button("×", key=f"_clr_{key}", help="クリア"):
        st.session_state[ss_key] = ""
        st.session_state.pop(key, None)
        st.rerun()
    return st.session_state[ss_key]
