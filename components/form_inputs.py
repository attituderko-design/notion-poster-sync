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
    clear_flag = f"_clr_pending_{key}"

    # クリア要求は次runの描画前に反映（Widget生成後のstate書き換えエラー回避）
    if st.session_state.get(clear_flag):
        st.session_state[ss_key] = ""
        st.session_state.pop(key, None)
        st.session_state.pop(clear_flag, None)

    if ss_key not in st.session_state:
        st.session_state[ss_key] = value
    elif refresh_on_value_change and value != st.session_state.get(ss_key, ""):
        st.session_state[ss_key] = value
        st.session_state.pop(key, None)

    host = (container or st)
    # ラベル行と入力行を明示的に分けて、ボタン位置を揃える
    if label:
        lbl_col, _ = host.columns([12, 1])
        lbl_col.markdown(f"**{label}**")
    inp_col, btn_col = host.columns([12, 1])
    if key in st.session_state:
        val = inp_col.text_input(
            label,
            placeholder=placeholder,
            key=key,
            label_visibility="collapsed",
            **kwargs,
        )
    else:
        val = inp_col.text_input(
            label,
            value=st.session_state[ss_key],
            placeholder=placeholder,
            key=key,
            label_visibility="collapsed",
            **kwargs,
        )
    st.session_state[ss_key] = val
    btn_col.markdown("&nbsp;", unsafe_allow_html=True)
    if btn_col.button("×", key=f"_clr_{key}", help="クリア"):
        st.session_state[clear_flag] = True
        st.rerun()
    return st.session_state[ss_key]
