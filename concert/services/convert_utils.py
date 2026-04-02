"""
concert/services/convert_utils.py
PDF→PNG変換とレポート出力UIの共通ユーティリティ。
"""
import io
import streamlit as st


def pdf_to_png_pages(pdf_bytes: bytes, dpi: int = 150) -> list[bytes]:
    """PDFバイト列を各ページのPNGバイト列リストに変換する。"""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = []
        for page in doc:
            mat = fitz.Matrix(dpi / 72, dpi / 72)
            pix = page.get_pixmap(matrix=mat)
            pages.append(pix.tobytes("png"))
        doc.close()
        return pages
    except ImportError:
        st.error("PyMuPDFがインストールされていません。requirements.txtに`PyMuPDF`を追加してください。")
        return []
    except Exception as e:
        st.error(f"PDF→PNG変換に失敗しました: {e}")
        return []


def render_report_output(
    pdf_bytes: bytes,
    filename: str,
    label: str = "レポート",
    key_prefix: str = "report",
):
    """
    ラジオボタンでPDF/画像を選択してダウンロードまたはプレビュー表示する共通UI。

    Args:
        pdf_bytes: 生成済みPDFのバイト列
        filename: ダウンロード時のファイル名（拡張子なし）
        label: UI上の表示ラベル
        key_prefix: Streamlitウィジェットのkeyプレフィックス
    """
    if not pdf_bytes:
        st.error("PDFの生成に失敗しました。")
        return

    fmt = st.radio(
        "出力形式",
        ["📄 PDF", "🖼 画像（PNG）"],
        horizontal=True,
        key=f"{key_prefix}_fmt",
    )

    if fmt == "📄 PDF":
        st.download_button(
            label=f"⬇️ {label}をダウンロード（PDF）",
            data=pdf_bytes,
            file_name=f"{filename}.pdf",
            mime="application/pdf",
            use_container_width=True,
            key=f"{key_prefix}_dl_pdf",
        )
    else:
        with st.spinner("画像に変換中..."):
            pages = pdf_to_png_pages(pdf_bytes)
        if pages:
            st.caption(f"{len(pages)}ページ / 右クリック（PC）または長押し（スマートフォン）で画像を保存できます。")
            for i, png in enumerate(pages):
                st.image(png, caption=f"p.{i+1}", use_container_width=True)
