/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/
import React, { Component, PropTypes } from 'react';
require('./KeyValuePairs.less');
import T from 'i18n-react';

class KeyValuePair extends Component {

  constructor(props){
    super(props);
    this.keyDown = this.keyDown.bind(this);
  }
  keyDown(e) {
    if(e.keyCode === 13){
      this.props.addRow();
    }
  }
  render() {
    return (
      <div className="key-value-pair-preference">
        <input type="text" value={this.props.name} autoFocus={true} onKeyDown={this.keyDown} onChange={this.props.onChange.bind(null, 'key')} placeholder={T.translate('commons.keyValPairs.keyPlaceholder')} className="key-input" />
        <input type="text" value={this.props.value} onKeyDown={this.keyDown} onChange={this.props.onChange.bind(null, 'value')} placeholder={T.translate('commons.keyValPairs.valuePlaceholder')} className="value-input" />
        <button type="submit" className="fa fa-plus add-row-btn" onClick={this.props.addRow} />
        <button type="submit" className="fa fa-trash remove-row-btn" onClick={this.props.removeRow} />
      </div>
    );
  }
}

KeyValuePair.propTypes = {
  className: PropTypes.string,
  name: PropTypes.string,
  value: PropTypes.string,
  onChange: PropTypes.func,
  addRow: PropTypes.func,
  removeRow: PropTypes.func
};

export default KeyValuePair;
