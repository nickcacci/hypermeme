import streamlit as st
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from meme_analysis_pipeline import pipeline as meme_pipeline
import tempfile


@st.cache_resource
def createPipeline():
    return meme_pipeline.createGooglePipeline(use_dummy_db=True)


def handle_upload():
    st.session_state.uploaded_file = st.session_state["uploader"]


def main():
    st.title("Meme Analyzer Demo")

    if "uploaded_file" not in st.session_state:
        st.session_state.uploaded_file = None

    pipeline = createPipeline()

    if st.session_state.uploaded_file is None:
        # Layout con file uploader a larghezza intera
        uploaded_file = st.file_uploader(
            "Upload an image",
            type=["jpg", "jpeg", "png"],
            key="uploader",
            on_change=handle_upload,
        )
    else:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp_file:
            tmp_file.write(st.session_state.uploaded_file.getvalue())
            tmp_file_path = tmp_file.name
            print("Uploaded file: ", tmp_file_path)

            with st.container():
                st.subheader("Meme Analysis")

                caption = "Meme"

                col1, col2 = st.columns([1, 2])
                with col1:
                    st.image(
                        st.session_state.uploaded_file,
                        caption=caption,
                        use_container_width=True,
                    )

                with col2:

                    with st.spinner("Analyzing the meme..."):
                        enriched_meme = pipeline.process_meme(tmp_file_path)
                    st.write(
                        f"**Visual Description:** {enriched_meme.enrichment.visual_description}"
                    )
                    st.write(f"**Image Text:** {enriched_meme.enrichment.image_text}")
                    st.write(f"**Explainer:** {enriched_meme.enrichment.explainer}")
                    st.write(f"**Tags:** {', '.join(enriched_meme.enrichment.tags)}")
                    caption = enriched_meme.enrichment.explainer
            st.success("Image successfully uploaded!")
            st.file_uploader(
                "Upload another meme",
                type=["png", "jpg", "jpeg"],
                key="uploader",
                on_change=handle_upload,
            )


if __name__ == "__main__":
    main()
