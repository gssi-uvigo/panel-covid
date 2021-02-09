import React from "react";
import Input from "antd/lib/input";
import CopyOutlinedIcon from "@ant-design/icons/CopyOutlined";
import Tooltip from "antd/lib/tooltip";

export default class InputWithCopy extends React.Component {
  constructor(props) {
    super(props);
    this.state = { copied: null };
    this.ref = React.createRef();
    this.copyFeatureSupported = document.queryCommandSupported("copy");
    this.resetCopyState = null;
  }

  componentWillUnmount() {
    if (this.resetCopyState) {
      clearTimeout(this.resetCopyState);
    }
  }

  copy = () => {
    // select text
    this.ref.current.select();

    // copy
    try {
      const success = document.execCommand("copy");
      if (!success) {
        throw new Error();
      }
      this.setState({ copied: "Copied!" });
    } catch (err) {
      this.setState({
        copied: "Copy failed",
      });
    }

    // reset tooltip
    this.resetCopyState = setTimeout(() => this.setState({ copied: null }), 2000);
  };

  render() {
    const copyButton = (
      <Tooltip title={this.state.copied || "Copy"}>
        <CopyOutlinedIcon style={{ cursor: "pointer" }} onClick={this.copy} />
      </Tooltip>
    );

    return <Input {...this.props} ref={this.ref} addonAfter={this.copyFeatureSupported && copyButton} />;
  }
}
