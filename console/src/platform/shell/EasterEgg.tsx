// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Easter egg componentry

import { chakra } from "@chakra-ui/react";
import React, { useEffect, useRef, useState } from "react";

import { useToast } from "~/hooks/useToast";

const cdnRoot = "https://de62f3dtfyis.cloudfront.net/";

type Track = {
  filePath: string;
  artist: string;
  attribution: string;
  title: string;
};

const tracks: Track[] = [
  {
    filePath: "vacation-2023.mp3",
    artist: "Jared Wofford & Josh Arenberg",
    attribution: "https://linktr.ee/jaredwoffordandjosharenberg",
    title: "🌊⋎⍲🝌⧜Ϯ⎞⍬𐒐🌊⤳",
  },
];

function getRandomTrack(): Track {
  return tracks[0];
}

const Audio = () => {
  const [track, _setTrack] = useState<Track>(getRandomTrack());
  const audio = useRef<HTMLAudioElement>(null);
  const toast = useToast({ position: "top", isClosable: true });
  // eslint-disable-next-line react-compiler/react-compiler
  // eslint-disable-next-line react-hooks/exhaustive-deps, react-compiler/react-compiler
  const toastCb = React.useCallback(toast, []);
  useEffect(() => {
    toastCb({
      duration: 5000,
      icon: null,
      status: "info",
      title: track.title,
      description: (
        <a href={track.attribution} target="_blank" rel="noreferrer">
          {track.artist}
        </a>
      ),
    });
  }, [track, toastCb]);
  return (
    <chakra.audio
      ref={audio}
      src={cdnRoot + track.filePath}
      autoPlay
      loop
    ></chakra.audio>
  );
};

export { Audio };
