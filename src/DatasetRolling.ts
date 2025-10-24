import { Dataset } from './Dataset.js';
import { DimensionName, RollingOptions } from './types.js';

export class DatasetRolling {
  private readonly _dimIndex: number;
  private readonly _options: RollingOptions;
  private readonly _window: number;

  constructor(
    private readonly _source: Dataset,
    private readonly _dim: DimensionName,
    window: number,
    options: RollingOptions
  ) {
    if (window <= 0 || !Number.isFinite(window)) {
      throw new Error('rolling window must be a positive integer');
    }
    this._window = Math.floor(window);
    this._options = options;

    const anyVar = _source.dataVars.find(name => _source.getVariable(name).dims.includes(_dim));
    if (!anyVar) {
      throw new Error(`Dimension '${_dim}' not found in any data variable`);
    }
    this._dimIndex = _source.getVariable(anyVar).dims.indexOf(_dim);
  }

  mean(): Dataset {
    return this._apply('mean');
  }

  sum(): Dataset {
    return this._apply('sum');
  }

  private _apply(reducer: 'mean' | 'sum'): Dataset {
    const newDataVars: { [name: string]: any } = {};

    for (const name of this._source.dataVars) {
      const array = this._source.getVariable(name);

      if (!array.dims.includes(this._dim)) {
        newDataVars[name] = array;
        continue;
      }

      const rolled = array.rolling(this._dim, this._window, this._options)[reducer]();
      newDataVars[name] = rolled;
    }

    return new Dataset(newDataVars, {
      coords: this._source.coords,
      attrs: this._source.attrs,
      coordAttrs: this._source.coordAttrs
    });
  }
}
