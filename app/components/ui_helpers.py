import streamlit as st
from data_lark.client import send_payload

def render_failure_summary(results):
    st.subheader("Failure Summary")
    failures = [r for r in results if not r["success"]]
    if not failures:
        st.success("✅ All checks passed")
        return

    for r in failures:
        st.markdown(f"**{r['expectation_type']} – {r.get('column')}**")
        st.dataframe(r["failed_materials"], use_container_width=True)

def render_send_to_datalark_button(payload):
    if st.button("🚀 Send Failures to Data Lark"):
        success, message = send_payload(payload)
        if success:
            st.success("✅ Sent successfully!")
        else:
            st.error(f"❌ Failed to send to Data Lark: {message}")
